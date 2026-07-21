using PaymentGateway.Domain.Accounts;
using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Infrastructure.Persistence.Repositories;

/// <summary>
/// 账户仓储实现 —— 资金账户,乐观锁核心
/// 学习要点:
///   1. Update 方法手动实现乐观锁: WHERE id=@id AND version=@oldVersion
///      SqlSugar 5.x 没有内置 IsConcurrencyToken,需显式构造 WHERE 条件
///   2. ★ 版本号自增由 Repository 负责(基础设施层职责):
///      account.Version 是读出时的版本(业务方法不自增),
///      Repository 用 account.Version 作为 WHERE 的 oldVersion,
///      并将 account.Version 设为 oldVersion+1 供 SET 使用
///   3. 若受影响行数=0,说明版本冲突(其他事务已先更新),抛 ConcurrencyException
///   4. 资金变更前必须获取 ZK 分布式锁(见 M2 DualLockProvider)
/// </summary>
public class AccountRepository : IAccountRepository
{
    private readonly ISqlSugarClient _db;

    public AccountRepository(ISqlSugarClient db) => _db = db;

    public async Task<Account?> GetByIdAsync(long id, CancellationToken ct = default)
        => await _db.Queryable<Account>().FirstAsync(a => a.Id == id);

    public async Task<Account?> GetByMerchantIdAsync(long merchantId, CancellationToken ct = default)
        => await _db.Queryable<Account>().FirstAsync(a => a.MerchantId == merchantId);

    public async Task AddAsync(Account account, CancellationToken ct = default)
    {
        // SqlSugar 5.x 不会自动回填自增 ID,需显式赋值
        account.Id = await _db.Insertable(account).ExecuteReturnIdentityAsync();
    }

    /// <summary>
    /// 更新账户 —— 触发乐观锁校验
    /// 学习要点: 通过显式 WHERE 条件实现 CAS(Compare And Swap)
    ///   UPDATE accounts SET balance=@new, version=@newVersion, ...
    ///   WHERE id=@id AND version=@oldVersion
    ///   若行数=0,说明其他事务已先更新(版本号不匹配),抛 ConcurrencyException
    ///
    /// ★ 为什么用 account.Version 而非 Version-1?
    ///   业务方法(Credit/Debit/Freeze/Unfreeze)不再自增 Version,
    ///   所以 account.Version 就是读出时的旧版本,直接作为 WHERE 条件。
    ///   这样一个事务内可连续调用多次业务方法(如退款流程的 Freeze+Unfreeze+Debit),
    ///   每次 Update 都能正确匹配当时的 DB 版本,避免误报冲突。
    /// </summary>
    public void Update(Account account)
    {
        // account.Version 是读出时的版本(业务方法未修改它)
        var oldVersion = account.Version;

        // 设置新版本号,供 Updateable 的 SET version=account.Version 使用
        account.SetVersion(oldVersion + 1);

        // Updateable 默认会把实体所有非主键字段更新到 DB(含 version)
        // Where 追加 WHERE 条件实现乐观锁: WHERE id=@id AND version=@oldVersion
        var rows = _db.Updateable(account)
            .Where(a => a.Id == account.Id && a.Version == oldVersion)
            .ExecuteCommand();

        if (rows == 0)
        {
            // 乐观锁冲突:其他事务已先更新该账户
            // 学习要点: 抛 ConcurrencyException 而非 InvalidOperationException
            //   专用异常类型便于 ExceptionMiddleware 精确映射 HTTP 409
            throw new ConcurrencyException("账户", account.Id);
        }
    }

    public async Task AddTransactionAsync(AccountTransaction tx, CancellationToken ct = default)
    {
        // SqlSugar 5.x 不会自动回填自增 ID,需显式赋值
        tx.Id = await _db.Insertable(tx).ExecuteReturnIdentityAsync();
    }

    /// <summary>
    /// 按业务单号查询流水 (幂等校验)
    /// </summary>
    public async Task<AccountTransaction?> GetTransactionByBizNoAsync(
        string bizNo, CancellationToken ct = default)
        => await _db.Queryable<AccountTransaction>().FirstAsync(t => t.BizNo == bizNo);
}
