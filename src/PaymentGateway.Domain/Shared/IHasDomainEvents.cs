namespace PaymentGateway.Domain.Shared;

/// <summary>
/// 拥有领域事件的对象(聚合根实现此接口)
/// 学习要点:
///   将此接口独立于 AggregateRoot 泛型基类,允许 IDomainEventDispatcher 以非泛型方式访问
///   (否则 Dispatcher 需要 DispatchAsync&lt;TId&gt; 泛型方法,使用不便)
/// </summary>
public interface IHasDomainEvents
{
    IReadOnlyCollection<IDomainEvent> DomainEvents { get; }

    /// <summary>事务提交后,由 Dispatcher 调用以清空已发布事件</summary>
    void ClearDomainEvents();
}
