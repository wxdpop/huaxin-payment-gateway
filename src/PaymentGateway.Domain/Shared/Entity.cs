using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Shared;

/// <summary>
/// 实体基类 —— DDD 中"有唯一标识"的对象
/// 学习要点: 实体 vs 值对象
///   - 实体: 有 Id,即使其他属性相同也是不同对象(如两个同名用户)
///   - 值对象: 无 Id,按属性值判断相等(如金额 100元 == 另一个 100元)
/// 实体相等性只看 Id,不看属性(保证两个引用同一聚合根的对象相等)
/// </summary>
/// <typeparam name="TId">主键类型(long/Guid 等)</typeparam>
public abstract class Entity<TId> where TId : notnull
{
    /// <summary>
    /// 主键(SqlSugar 自增标识)
    /// 学习要点: 此处用 set 而非 protected set,因为 SqlSugar 通过反射回填自增 ID 时
    ///   无法访问非 public 的 setter。DDD 纯粹主义会用 protected set + 仓储内反射,
    ///   但实际工程中 ORM 兼容性更重要,Id 的不可变性通过约定保证(外部不主动赋值)
    /// </summary>
    [SugarColumn(IsPrimaryKey = true, IsIdentity = true, ColumnName = "id")]
    public TId Id { get; set; } = default!;

    /// <summary>
    /// 实体相等性比较 —— 仅比较 Id 与类型
    /// 两个 Order 实例 Id 相同即视为同一实体,即使其他属性不同
    /// </summary>
    public override bool Equals(object? obj)
    {
        if (obj is not Entity<TId> other) return false;
        if (ReferenceEquals(this, other)) return true;
        if (GetType() != other.GetType()) return false;  // 不同类型不相等
        return EqualityComparer<TId>.Default.Equals(Id, other.Id);
    }

    public override int GetHashCode() => Id.GetHashCode();

    public static bool operator ==(Entity<TId>? a, Entity<TId>? b) =>
        a?.Equals(b) ?? b is null;

    public static bool operator !=(Entity<TId>? a, Entity<TId>? b) => !(a == b);
}

/// <summary>
/// 领域事件标记接口
/// 学习要点: 领域事件用于"解耦"聚合根之间的通信
///   如 Order.MarkAsPaid() 产生 OrderPaidEvent,由应用层在事务提交后发布到 Kafka
///   聚合根不关心谁消费事件,只负责"发生了什么"
/// </summary>
public interface IDomainEvent
{
    Guid EventId { get; }
    DateTimeOffset OccurredAt { get; }
}
