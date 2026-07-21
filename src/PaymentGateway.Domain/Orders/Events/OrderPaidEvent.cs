using PaymentGateway.Domain.Shared;

namespace PaymentGateway.Domain.Orders.Events;

/// <summary>
/// 订单支付成功事件 —— 收到渠道成功回调后产生
/// 学习要点: 该事件触发异步记账流程(Kafka消费者)
///   账户模块消费此事件,执行余额变更+流水写入
/// </summary>
public record OrderPaidEvent(
    long OrderId,
    string OrderNo,
    long MerchantId,
    decimal Amount) : IDomainEvent
{
    public Guid EventId { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredAt { get; } = DateTimeOffset.UtcNow;
}
