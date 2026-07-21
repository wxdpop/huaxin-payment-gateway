namespace PaymentGateway.Domain.Accounts;

/// <summary>
/// 账户仓储接口 —— 领域层定义,基础设施层实现
/// 学习要点:
///   1. GetByMerchantIdAsync: 按商户ID查询账户(下单/回调时获取商户账户)
///   2. Update 时通过 Version 字段实现乐观锁(SqlSugar 手动构造 WHERE version=@old)
///   3. 账户余额变更前必须获取 ZK 分布式锁(见 Infrastructure/DistributedLock)
/// </summary>
public interface IAccountRepository
{
    Task<Account?> GetByIdAsync(long id, CancellationToken ct = default);

    /// <summary>按商户ID查询账户(下单/退款时获取商户资金账户)</summary>
    Task<Account?> GetByMerchantIdAsync(long merchantId, CancellationToken ct = default);

    Task AddAsync(Account account, CancellationToken ct = default);

    /// <summary>
    /// 更新账户 —— 触发乐观锁校验
    /// 学习要点: SqlSugar 5.x 无内置 IsConcurrencyToken,在实现类中手动构造 WHERE 条件
    ///   UPDATE accounts SET balance=@new, version=version+1
    ///   WHERE id=@id AND version=@oldVersion
    ///   若行数=0 表示版本冲突,抛 InvalidOperationException(由调用方重试)
    /// </summary>
    void Update(Account account);

    /// <summary>添加账户流水(同一事务内,确保余额与流水一致)</summary>
    Task AddTransactionAsync(AccountTransaction tx, CancellationToken ct = default);

    /// <summary>
    /// 按业务单号(biz_no)查询流水 —— 用于入账幂等校验
    /// 学习要点: 入账消费者通过此方法判断是否已入账,避免重复记账
    ///   流水表 biz_no 唯一约束是 DB 层兜底,业务层查询是性能优化
    /// </summary>
    Task<AccountTransaction?> GetTransactionByBizNoAsync(string bizNo, CancellationToken ct = default);
}
