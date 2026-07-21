using PaymentGateway.Domain.Shared;

namespace PaymentGateway.Domain.Orders.Events;

/// <summary>
/// 订单创建事件 —— 订单创建后产生,通知路由模块选择渠道
/// 学习要点: 用 record 定义事件(不可变),实现 IDomainEvent
///   应用层在事务提交后取出该事件发布到 Kafka
/// </summary>
public record OrderCreatedEvent(
    long OrderId,
    string OrderNo,
    long MerchantId) : IDomainEvent
{
    public Guid EventId { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredAt { get; } = DateTimeOffset.UtcNow;
}
