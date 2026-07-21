using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Domain.Orders;
using PaymentGateway.Domain.Payments;
using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Application.Payments;

// ============================================================================
// PayOrderService —— 发起支付应用服务
// ============================================================================
// ★ 学习要点: 模拟渠道调用流程
//   1. 查询订单,校验状态(Pending)
//   2. 模拟调用第三方渠道(微信/支付宝),生成渠道预支付订单号
//   3. 创建 PaymentRecord(待回调)
//   4. 更新订单状态为 Paying
//   5. 返回支付凭证(渠道订单号 + 模拟支付链接)
//
// 【真实场景 vs 学习工程】
//   真实场景: 调用微信/支付宝的下单 API,获取 prepay_id
//   学习工程: 用 Guid 模拟渠道订单号,自动模拟"支付成功回调"
// ============================================================================

/// <summary>发起支付服务接口</summary>
public interface IPayOrderService
{
    Task<PayOrderResult> PayAsync(PayOrderCommand command, CancellationToken ct = default);
}

/// <summary>发起支付命令</summary>
public record PayOrderCommand(long OrderId, string ChannelCode);

/// <summary>发起支付结果</summary>
public record PayOrderResult(
    long OrderId,
    string OrderNo,
    string ChannelCode,
    string ChannelOrderNo,
    string PayUrl,
    string Status);

public class PayOrderService : IPayOrderService
{
    private readonly IOrderRepository _orderRepository;
    private readonly IPaymentRepository _paymentRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<PayOrderService> _logger;

    public PayOrderService(
        IOrderRepository orderRepository,
        IPaymentRepository paymentRepository,
        IUnitOfWork unitOfWork,
        ILogger<PayOrderService> logger)
    {
        _orderRepository = orderRepository;
        _paymentRepository = paymentRepository;
        _unitOfWork = unitOfWork;
        _logger = logger;
    }

    public async Task<PayOrderResult> PayAsync(PayOrderCommand command, CancellationToken ct = default)
    {
        // 1. 查询订单
        var order = await _orderRepository.GetByIdAsync(command.OrderId, ct)
            ?? throw new BusinessException($"订单 {command.OrderId} 不存在");

        // 2. 校验订单状态(只有 Pending 状态才能发起支付)
        if (order.Status != OrderStatus.Pending)
            throw new BusinessException($"订单状态 {order.Status} 不允许发起支付", "INVALID_ORDER_STATUS");

        // 3. 模拟渠道调用 —— 生成渠道预支付订单号
        //    学习要点: 真实场景调用微信/支付宝下单 API,这里用 Guid 模拟
        var channelOrderNo = $"CH{DateTimeOffset.UtcNow:yyyyMMddHHmmss}{Random.Shared.Next(100000, 999999)}";
        var payUrl = $"https://mock-pay.example.com/pay?orderNo={channelOrderNo}";

        _logger.LogInformation("模拟渠道调用: channel={Channel}, orderNo={OrderNo}, channelOrderNo={ChannelOrderNo}",
            command.ChannelCode, order.OrderNo, channelOrderNo);

        // 4. 创建支付记录(待回调)
        //    学习要点: PaymentRecord 在支付发起时创建,回调时更新
        var paymentRecord = PaymentRecord.Create(
            order.Id, command.ChannelCode, channelOrderNo, order.Amount);
        await _paymentRepository.AddAsync(paymentRecord, ct);

        // 5. 更新订单状态为 Paying
        //    学习要点: MarkAsPaying 内部校验状态机(Pending → Paying)
        //    同时填充 ChannelCode 和 ChannelOrderNo
        order.MarkAsPaying(command.ChannelCode, channelOrderNo);
        _orderRepository.Update(order);

        // 6. 保存事务
        await _unitOfWork.SaveChangesAsync(ct);

        _logger.LogInformation("支付发起成功: orderNo={OrderNo}, channelOrderNo={ChannelOrderNo}",
            order.OrderNo, channelOrderNo);

        return new PayOrderResult(
            order.Id,
            order.OrderNo,
            command.ChannelCode,
            channelOrderNo,
            payUrl,
            order.Status.ToString());
    }
}
