namespace PaymentGateway.Application.Abstractions;

/// <summary>
/// 工作单元(Unit of Work)接口 —— 应用层定义,基础设施层实现
/// 学习要点:
///   1. UoW 模式: 保证一次业务操作内多个仓储变更在同一事务内提交(原子性)
///      例如: 创建订单 + 扣减库存 + 记录流水 必须同时成功或同时失败
///   2. 领域事件在事务提交成功后发布,避免"幻读事件"问题
///   3. ★ SqlSugar 无 ChangeTracker,每次操作即时自动提交(非事务)
///      单表写入无需显式事务; 多表原子操作必须用 ExecuteInTransactionAsync 包裹,
///      否则中途异常会导致部分提交(资金脏数据)!
/// </summary>
public interface IUnitOfWork
{
    /// <summary>
    /// 提交事务,返回受影响行数
    /// 注意: SqlSugar 无 ChangeTracker,此方法为空操作(每次操作已即时执行)
    ///   真正的事务边界由 ExecuteInTransactionAsync 控制
    /// </summary>
    Task<int> SaveChangesAsync(CancellationToken ct = default);

    /// <summary>
    /// 在数据库事务内执行操作 (保证原子性: 全部成功或全部回滚)
    /// 学习要点: 资金类业务(退款/入账)必须用此方法包裹,避免中途异常导致部分提交
    ///   - 事务内所有仓储操作共享同一连接与事务
    ///   - action 抛异常 → 自动回滚; 正常返回 → 自动提交
    ///   - 事件发布应放在事务外(获取返回值后),避免"事务回滚但消息已发"
    /// </summary>
    /// <typeparam name="T">操作返回值类型</typeparam>
    /// <param name="action">事务内执行的业务逻辑</param>
    /// <param name="ct">取消令牌</param>
    Task<T> ExecuteInTransactionAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken ct = default);
}
