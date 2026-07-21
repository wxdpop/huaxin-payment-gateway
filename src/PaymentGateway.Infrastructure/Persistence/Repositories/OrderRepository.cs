using PaymentGateway.Domain.Orders;
using SqlSugar;

namespace PaymentGateway.Infrastructure.Persistence.Repositories;

/// <summary>
/// 订单仓储实现 —— SqlSugar 实现 IOrderRepository
/// 学习要点:
///   1. Queryable&lt;T&gt;(): 链式查询,类似 LINQ,底层生成参数化 SQL
///   2. Insertable&lt;T&gt;(entity): 插入,自动忽略 IsIdentity=true 的主键
///      ExecuteReturnIdentityAsync() 返回 LAST_INSERT_ID() 值(需手动赋给实体)
///      注意: SqlSugar 5.x 不会自动回填自增 ID 到实体,必须显式捕获返回值并赋值
///   3. Updateable&lt;T&gt;(entity): 更新,默认更新所有非主键字段
///   4. SqlSugar 无 EFCore 那种 ChangeTracker,每次操作显式调用,更直观
/// </summary>
public class OrderRepository : IOrderRepository
{
    private readonly ISqlSugarClient _db;

    public OrderRepository(ISqlSugarClient db) => _db = db;

    // 学习要点: FirstAsync 在 SqlSugar 中返回 Task<T>(非 nullable)
    //   但查询可能返回 null(无匹配行),用 async/await 显式转换为 Order? 以消除 nullable 警告
    public async Task<Order?> GetByIdAsync(long id, CancellationToken ct = default)
        => await _db.Queryable<Order>().FirstAsync(o => o.Id == id);

    public async Task<Order?> FindByOrderNoAsync(string orderNo, CancellationToken ct = default)
        => await _db.Queryable<Order>().FirstAsync(o => o.OrderNo == orderNo);

    public async Task<Order?> FindByChannelOrderNoAsync(string channelOrderNo, CancellationToken ct = default)
        => await _db.Queryable<Order>().FirstAsync(o => o.ChannelOrderNo == channelOrderNo);

    public async Task AddAsync(Order order, CancellationToken ct = default)
    {
        // 学习要点: Insertable 自动忽略 IsIdentity=true 的自增主键列
        //   ExecuteReturnIdentityAsync 返回 MySQL LAST_INSERT_ID() 的 long 值
        //   SqlSugar 5.x 不会自动回填到实体,必须显式赋值给 order.Id
        order.Id = await _db.Insertable(order).ExecuteReturnIdentityAsync();
    }

    /// <summary>
    /// 同步更新(SqlSugar 同步 API)
    /// 学习要点: Updateable 默认更新所有非主键字段,WHERE id=@id
    /// </summary>
    public void Update(Order order) =>
        _db.Updateable(order).ExecuteCommand();
}
