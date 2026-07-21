using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Application.EventBus;
using PaymentGateway.Domain.Orders;
using PaymentGateway.Domain.Payments;
using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Application.Payments;

// ============================================================================
// HandleCallbackService —— 支付回调处理应用服务
// ============================================================================
// ★ 学习要点: 这是支付网关"回调幂等"的核心业务链路,串联多个技术点
//
// 【完整回调链路】
//   1. 接收渠道回调 (Api 层 CallbackEndpoints)
//   2. 获取 Redis 分布式锁 (channel_order_no 为 key) ← M2 已实现
//   3. 幂等校验 (DB 层 payment_records 唯一约束)
//   4. 更新订单状态 (Order.MarkAsPaid)
//   5. 更新支付记录 (PaymentRecord.MarkSuccess)
//   6. 发布 PaymentSucceededEvent 到 Kafka ← M3 实现
//   7. 异步消费者消费 PaymentSucceededEvent → 入账 (CreditAccountConsumer)
//   8. 入账完成发布 AccountCreditedEvent → 商户通知 (MerchantNotifyConsumer)
//
// 【为什么先加 Redis 锁再查 DB?】
//   - 高并发场景下,多个重复回调同时到达,DB 还没写入 payment_records 时
//     3 个请求都查不到记录,会都尝试写入 → 触发 DB 唯一约束冲突
//   - Redis 锁先拦截: 第一个获取锁的处理,其他快速失败返回
//   - 学习要点: 多层防护,DB 唯一约束是兜底,Redis 锁是性能优化
//
// 【为什么不在锁内完成入账?】
//   - 入账涉及资金变更 (DB 事务 + ZK 锁 + 账户余额更新),耗时长
//   - 锁内只做轻量操作 (状态变更 + Kafka 发布),快速释放锁
//   - 异步通过 Kafka 触发入账,解耦回响延迟与资金处理
// ============================================================================

public class HandleCallbackService
{
    private readonly IDistributedLock _distributedLock;
    private readonly IOrderRepository _orderRepository;
    private readonly IPaymentRepository _paymentRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly IEventBus _eventBus;
    private readonly ILogger<HandleCallbackService> _logger;

    public HandleCallbackService(
        IDistributedLock distributedLock,
        IOrderRepository orderRepository,
        IPaymentRepository paymentRepository,
        IUnitOfWork unitOfWork,
        IEventBus eventBus,
        ILogger<HandleCallbackService> logger)
    {
        _distributedLock = distributedLock;
        _orderRepository = orderRepository;
        _paymentRepository = paymentRepository;
        _unitOfWork = unitOfWork;
        _eventBus = eventBus;
        _logger = logger;
    }

    /// <summary>
    /// 处理支付回调 (主入口)
    /// </summary>
    public async Task<HandleCallbackResult> HandleAsync(
        HandleCallbackRequest request,
        CancellationToken ct = default)
    {
        // ★ Step 1: 获取 Redis 分布式锁 (channel_order_no 为 key)
        //   学习要点: 锁 Key 选择 — 必须是全局唯一稳定的业务键
        //   channel_order_no 是渠道返回的全局唯一订单号,符合要求
        //   过期时间 5s (回调处理通常很快,5s 足够)
        var lockKey = $"callback:{request.ChannelCode}:{request.ChannelOrderNo}";
        await using var lockHandle = await _distributedLock.TryAcquireAsync(
            lockKey, TimeSpan.FromSeconds(5), ct);

        if (lockHandle == null)
        {
            // Redis 锁获取失败 → 说明已有其他实例在处理同一回调
            //   学习要点: 返回"处理中"状态,由调用方决定是否重试
            _logger.LogInformation("回调处理中(锁被占用): channelOrderNo={ChannelOrderNo}",
                request.ChannelOrderNo);
            return HandleCallbackResult.Processing;
        }

        // ★ Step 2: 幂等校验 (查 payment_records 是否已处理)
        var existingRecord = await _paymentRepository.FindByChannelOrderNoAsync(
            request.ChannelCode, request.ChannelOrderNo, ct);

        if (existingRecord?.Status == PaymentStatus.Success)
        {
            // 已成功处理过 → 直接返回成功 (幂等)
            _logger.LogInformation("回调已处理过(幂等): channelOrderNo={ChannelOrderNo}",
                request.ChannelOrderNo);
            return HandleCallbackResult.AlreadyHandled;
        }

        // ★ Step 3: 查询订单
        var order = await _orderRepository.FindByChannelOrderNoAsync(
            request.ChannelOrderNo, ct);
        if (order == null)
        {
            _logger.LogWarning("回调对应的订单不存在: channelOrderNo={ChannelOrderNo}",
                request.ChannelOrderNo);
            throw new BusinessException("订单不存在");
        }

        // ★ Step 4: 业务校验 (金额一致)
        if (order.Amount.Value != request.Amount)
        {
            _logger.LogError("回调金额与订单金额不一致: orderNo={OrderNo}, orderAmount={Order}, callbackAmount={Callback}",
                order.OrderNo, order.Amount.Value, request.Amount);
            throw new BusinessException("回调金额与订单金额不一致");
        }

        // ★ Step 5: 更新订单状态 (Pending/Paying → Paid)
        if (order.Status != OrderStatus.Paid)
        {
            order.MarkAsPaid();
            _orderRepository.Update(order);
        }

        // ★ Step 6: 创建/更新支付记录
        if (existingRecord == null)
        {
            // 首次处理: 创建支付记录
            existingRecord = PaymentRecord.Create(
                order.Id, request.ChannelCode, request.ChannelOrderNo, order.Amount);
            existingRecord.MarkSuccess(request.ChannelTradeNo, request.CallbackRaw);
            await _paymentRepository.AddAsync(existingRecord, ct);
        }
        else
        {
            // 已有记录: 标记成功
            existingRecord.MarkSuccess(request.ChannelTradeNo, request.CallbackRaw);
            _paymentRepository.Update(existingRecord);
        }

        // ★ Step 7: 保存事务 (SqlSugar UnitOfWork)
        await _unitOfWork.SaveChangesAsync(ct);

        _logger.LogInformation("回调处理成功: orderNo={OrderNo}, channelOrderNo={ChannelOrderNo}",
            order.OrderNo, request.ChannelOrderNo);

        // ★ Step 8: 发布 PaymentSucceededEvent 到 Kafka
        //   学习要点: 事务提交后再发事件,避免事务回滚但消息已发出
        //   生产场景: 应在事务内写 Outbox 表,后台任务异步发送 (Outbox 模式)
        //   本工程简化: 直接发送,如失败抛异常让上层重试
        var paymentSucceededEvent = new PaymentSucceededEvent
        {
            OrderId = order.Id,
            OrderNo = order.OrderNo,
            MerchantId = order.MerchantId,
            Amount = order.Amount.Value,
            ChannelCode = order.ChannelCode ?? request.ChannelCode,
            ChannelOrderNo = request.ChannelOrderNo
        };

        await _eventBus.PublishAsync(
            PaymentEventTopics.PaymentSucceeded,
            paymentSucceededEvent,
            ct);

        // Redis 锁自动释放 (await using)
        return HandleCallbackResult.Success;
    }
}

// ============================================================================
// 请求/响应 DTO
// ============================================================================

public record HandleCallbackRequest(
    string ChannelCode,
    string ChannelOrderNo,
    string? ChannelTradeNo,
    decimal Amount,
    DateTimeOffset PaidAt,
    string CallbackRaw);

public enum HandleCallbackResult
{
    Success,
    AlreadyHandled,
    Processing
}
