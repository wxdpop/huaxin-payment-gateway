using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Application.EventBus;
using PaymentGateway.Domain.Accounts;
using PaymentGateway.Domain.Orders;
using PaymentGateway.Domain.Refunds;
using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Application.Payments;

// ============================================================================
// RefundOrderService —— 退款应用服务
// ============================================================================
// ★ 学习要点: 退款是资金逆向操作,涉及多步状态流转
//
// 【退款完整链路】
//   1. 查询订单,校验状态(Paid 才能退款)
//   2. 幂等校验: 检查是否已有退款记录
//   3. 获取 ZK 分布式锁(merchant_id 为 key) —— 资金变更必须加锁
//   4. 查询账户,冻结退款金额(Account.Freeze)
//   5. 创建退款记录(RefundRecord,状态 Pending)
//   6. 模拟渠道退款(学习工程自动成功)
//   7. 退款成功: 从冻结金额扣减(Account.Debit 不可用,需直接操作)
//   →   实际操作: FrozenAmount -= amount(通过 Unfreeze + Debit 组合)
//   8. 更新订单状态为 Refunded
//   9. 创建账户流水(Debit 类型)
//   10. 保存事务 + 发布退款事件
//
// 【资金安全三重保障】
//   1. ZK 分布式锁: 串行化同一商户的资金操作
//   2. DB 乐观锁: Account.Version CAS 校验
//   3. DB 唯一约束: refund_no 唯一,防止重复退款
// ============================================================================

/// <summary>退款服务接口</summary>
public interface IRefundOrderService
{
    Task<RefundOrderResult> RefundAsync(RefundOrderCommand command, CancellationToken ct = default);
}

/// <summary>退款命令</summary>
public record RefundOrderCommand(long OrderId, string? Reason);

/// <summary>退款结果</summary>
public record RefundOrderResult(
    long RefundId,
    string RefundNo,
    long OrderId,
    decimal Amount,
    string Status);

public class RefundOrderService : IRefundOrderService
{
    private readonly IOrderRepository _orderRepository;
    private readonly IAccountRepository _accountRepository;
    private readonly IRefundRepository _refundRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly IDistributedLock _distributedLock;
    private readonly IEventBus _eventBus;
    private readonly ILogger<RefundOrderService> _logger;

    public RefundOrderService(
        IOrderRepository orderRepository,
        IAccountRepository accountRepository,
        IRefundRepository refundRepository,
        IUnitOfWork unitOfWork,
        IDistributedLock distributedLock,
        IEventBus eventBus,
        ILogger<RefundOrderService> logger)
    {
        _orderRepository = orderRepository;
        _accountRepository = accountRepository;
        _refundRepository = refundRepository;
        _unitOfWork = unitOfWork;
        _distributedLock = distributedLock;
        _eventBus = eventBus;
        _logger = logger;
    }

    public async Task<RefundOrderResult> RefundAsync(RefundOrderCommand command, CancellationToken ct = default)
    {
        // 1. 查询订单
        var order = await _orderRepository.GetByIdAsync(command.OrderId, ct)
            ?? throw new BusinessException($"订单 {command.OrderId} 不存在");

        // 2. 校验订单状态(只有 Paid 才能退款)
        if (order.Status != OrderStatus.Paid)
            throw new BusinessException($"订单状态 {order.Status} 不允许退款", "INVALID_ORDER_STATUS");

        // 3. 幂等校验: 检查是否已退款
        var existingRefund = await _refundRepository.FindByOrderIdAsync(order.Id, ct);
        if (existingRefund is { Status: RefundStatus.Refunded })
            throw new BusinessException($"订单 {order.OrderNo} 已退款", "DUPLICATE_REFUND");

        // 4. 获取分布式锁(以 merchant_id 为 key)
        //    学习要点: 资金变更前必须加锁,避免并发导致余额错误
        var lockKey = $"account:{order.MerchantId}";
        await using var lockHandle = await _distributedLock.TryAcquireAsync(
            lockKey, TimeSpan.FromSeconds(10), ct);

        if (lockHandle == null)
        {
            _logger.LogWarning("退款 ZK 锁获取失败: merchantId={MerchantId}", order.MerchantId);
            throw new BusinessException("资金操作繁忙,请稍后重试", "LOCK_FAILED");
        }

        // ★ 5-16: 在数据库事务内执行资金变更 (保证原子性)
        //   学习要点: 退款涉及 账户冻结/扣减 + 退款记录 + 流水 多表写入,必须同事务
        //   ★ SqlSugar 默认每次操作自动提交(非事务),如不包事务:
        //     中途异常(如乐观锁冲突)会导致已执行的 Update 部分提交 → 资金脏数据!
        //   用 ExecuteInTransactionAsync 包裹,异常自动回滚所有 Update
        var (refundNo, refund, account) = await _unitOfWork.ExecuteInTransactionAsync(async ct =>
        {
            // 5. 查询账户
            var account = await _accountRepository.GetByMerchantIdAsync(order.MerchantId, ct)
                ?? throw new BusinessException($"商户 {order.MerchantId} 账户不存在");

            // 6. 冻结退款金额
            //    学习要点: Freeze 从可用余额转入冻结金额,退款成功后再扣减
            account.Freeze(order.Amount);

            // 7. 生成退款单号 + 创建退款记录
            var refundNo = $"RF{DateTimeOffset.UtcNow:yyyyMMddHHmmss}{Random.Shared.Next(100000, 999999)}";
            var refund = RefundRecord.Create(
                order.Id, order.MerchantId, order.Amount, refundNo, command.Reason);
            await _refundRepository.AddAsync(refund, ct);

            // 8. 更新账户(触发乐观锁 CAS 校验)
            //    ★ 事务内第一次 Update: WHERE version=读出版本,SET version=读出+1
            //      同一事务内同连接可见,后续 Update 能读到未提交的版本
            _accountRepository.Update(account);

            // 9. 创建冻结流水
            var freezeTx = AccountTransaction.Create(
                accountId: account.Id,
                orderId: order.Id,
                txType: TransactionType.Freeze,
                amount: order.Amount,
                balanceAfter: account.Balance,
                bizNo: refundNo,
                remark: $"退款冻结: {order.OrderNo}");
            await _accountRepository.AddTransactionAsync(freezeTx, ct);

            // 10. 模拟渠道退款(学习工程自动成功)
            var channelRefundNo = $"CRF{DateTimeOffset.UtcNow:yyyyMMddHHmmss}{Random.Shared.Next(100000, 999999)}";
            _logger.LogInformation("模拟渠道退款: refundNo={RefundNo}, channelRefundNo={ChannelRefundNo}",
                refundNo, channelRefundNo);

            // 11. 退款成功: 从冻结金额扣减
            //     学习要点: 先解冻(Unfreeze: 冻结→可用) 再出账(Debit: 可用→扣减)
            //     组合操作 = 从冻结金额直接扣减
            account.Unfreeze(order.Amount);
            account.Debit(order.Amount);

            // 12. 更新退款记录状态
            refund.MarkRefunded(channelRefundNo);

            // 13. 更新订单状态为 Refunded
            order.MarkAsRefunded();

            // 14. 再次更新账户(Version 再次自增)
            //    ★ 事务内第二次 Update: WHERE version=第一次更新后版本(同连接可见)
            _accountRepository.Update(account);
            _refundRepository.Update(refund);
            _orderRepository.Update(order);

            // 15. 创建扣款流水
            var debitTx = AccountTransaction.Create(
                accountId: account.Id,
                orderId: order.Id,
                txType: TransactionType.Debit,
                amount: order.Amount,
                balanceAfter: account.Balance,
                bizNo: $"{refundNo}-debit",
                remark: $"退款扣款: {order.OrderNo}");
            await _accountRepository.AddTransactionAsync(debitTx, ct);

            // 16. (事务由 ExecuteInTransactionAsync 自动提交,无需 SaveChanges)
            _logger.LogInformation(
                "退款成功: orderNo={OrderNo}, refundNo={RefundNo}, amount={Amount}, balanceAfter={Balance}",
                order.OrderNo, refundNo, order.Amount.Value, account.Balance.Value);

            return (refundNo, refund, account);
        }, ct);

        // 17. 发布退款成功事件 (事务外)
        //   学习要点: 事件发布在事务提交后,避免"事务回滚但消息已发"的幻读事件
        await _eventBus.PublishAsync(
            PaymentEventTopics.RefundRequested,
            new RefundSucceededEvent
            {
                OrderId = order.Id,
                OrderNo = order.OrderNo,
                MerchantId = order.MerchantId,
                RefundNo = refundNo,
                Amount = order.Amount.Value
            },
            ct);

        return new RefundOrderResult(
            refund.Id,
            refund.RefundNo,
            order.Id,
            order.Amount.Value,
            refund.Status.ToString());
    }
}
