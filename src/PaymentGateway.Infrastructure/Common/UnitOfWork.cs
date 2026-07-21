using SqlSugar;
using PaymentGateway.Application.Abstractions;

namespace PaymentGateway.Infrastructure.Common;

/// <summary>
/// 工作单元实现 —— 使用 SqlSugar 事务
/// 学习要点:
///   1. SqlSugar 事务: 通过 Ado.UseTranAsync() 开启,正常返回自动提交,抛异常自动回滚
///   2. ★ SqlSugar 默认每次操作自动提交(非事务),ExecuteInTransactionAsync 显式开启事务
///   3. SqlSugarScope(Scoped) + AsyncLocal,跨异步上下文共享同一事务
///   4. 事务内 ExecuteCommand 的乐观锁 WHERE version 检查对同一连接可见(读未提交)
/// </summary>
public class UnitOfWork : IUnitOfWork
{
    private readonly ISqlSugarClient _db;

    public UnitOfWork(ISqlSugarClient db) => _db = db;

    /// <summary>
    /// 提交事务 (SqlSugar 无 ChangeTracker,此方法为空操作)
    /// 真正的事务边界由 ExecuteInTransactionAsync 控制
    /// </summary>
    public Task<int> SaveChangesAsync(CancellationToken ct = default)
        => Task.FromResult(0);

    /// <summary>
    /// 在数据库事务内执行操作 (保证原子性)
    /// 学习要点: SqlSugar Ado.UseTranAsync 用法
    ///   - 接收 Func<Task>,内部所有 db 操作在同一事务内执行
    ///   - 正常返回 → 自动 CommitTran; 抛异常 → 自动 RollbackTran
    ///   - 返回 DbResult.IsSuccess 判断是否成功
    /// </summary>
    public async Task<T> ExecuteInTransactionAsync<T>(
        Func<CancellationToken, Task<T>> action,
        CancellationToken ct = default)
    {
        // ★ 学习要点: UseTranAsync 接收 Func<Task>(无返回值),用闭包局部变量捕获结果
        //   - action 内所有仓储操作(_db 共享同一 SqlSugarScope)在同一事务
        //   - action 抛异常(如乐观锁冲突 ConcurrencyException) → 自动回滚,所有 Update 失效
        //   - action 正常返回 → 自动提交
        T? resultValue = default;
        var tranResult = await _db.Ado.UseTranAsync(async () =>
        {
            resultValue = await action(ct);
        });

        if (!tranResult.IsSuccess)
        {
            // 事务失败: 保留原始异常(如 ConcurrencyException)向上抛出
            //   学习要点: 抛原始异常让 ExceptionMiddleware 精确映射 HTTP 状态码
            throw tranResult.ErrorException
                ?? new InvalidOperationException("事务执行失败,已回滚");
        }

        return resultValue!;
    }
}
