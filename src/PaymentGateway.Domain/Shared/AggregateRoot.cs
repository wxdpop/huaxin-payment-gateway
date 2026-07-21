using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Shared;

/// <summary>
/// 聚合根基类 —— DDD 聚合的"一致性边界"入口
/// 学习要点: 聚合根(Aggregate Root)是外部访问聚合的唯一入口
///   - 所有对聚合内实体的修改必须通过聚合根的方法
///   - 聚合根保证内部"不变量"(invariant)始终成立
///   - 例如 Order 聚合根保证"已支付的订单不能再次支付"
/// 领域事件收集在 _domainEvents,由应用层在 SaveChanges 后统一发布
/// </summary>
public abstract class AggregateRoot<TId> : Entity<TId>, IHasDomainEvents where TId : notnull
{
    /// <summary>待发布的领域事件列表(应用层事务提交后取出发布)</summary>
    private readonly List<IDomainEvent> _domainEvents = new();

    /// <summary>
    /// 领域事件列表(只读视图)
    /// 学习要点: [SugarColumn(IsIgnore = true)] 告诉 SqlSugar 此属性不参与持久化
    ///   领域事件是运行时概念,不应映射到数据库列
    ///   在基类统一声明 IsIgnore,子类无需重复
    /// </summary>
    [SugarColumn(IsIgnore = true)]
    public IReadOnlyCollection<IDomainEvent> DomainEvents => _domainEvents.AsReadOnly();

    /// <summary>添加领域事件(由聚合根内部业务方法调用)</summary>
    protected void AddDomainEvent(IDomainEvent evt)
    {
        if (evt is null) throw new DomainException("领域事件不能为空");
        _domainEvents.Add(evt);
    }

    /// <summary>应用层在事务提交后调用,清空已发布事件</summary>
    public void ClearDomainEvents() => _domainEvents.Clear();
}
