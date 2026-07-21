using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Accounts;

/// <summary>
/// 资金账户聚合根 —— 商户的资金账户,管理余额与冻结金额
/// 学习要点:
///   1. 账户余额变更必须通过业务方法(Credit/Debit/Freeze),封装校验逻辑
///   2. version 字段用于乐观锁(并发更新时版本冲突检测)
///   3. 所有金额操作用 Money 值对象,避免 decimal 散落
/// 资金安全: 余额变更前需获取 ZK 分布式锁(见 DualLockProvider)
/// ★ SqlSugar 乐观锁设计说明:
///   - SqlSugar 5.x 没有内置 IsConcurrencyToken(不像 EF Core 的 [Timestamp])
///   - 乐观锁 version 的自增由 AccountRepository.Update 负责(基础设施层职责)
///   - 领域层的业务方法(Credit/Debit/Freeze/Unfreeze)只修改业务字段(balance/frozen),
///     不自增 Version —— 这样一个事务内可连续调用多次业务方法,最后一次 Update 即可
///   - 旧实现: 业务方法 Version++ + Repository 用 oldVersion=Version-1,
///     仅在"单次业务方法+单次 Update"时正确,多次调用会算错 oldVersion 导致误报冲突
/// </summary>
[SugarTable("accounts")]
public class Account : AggregateRoot<long>
{
    [SugarColumn(ColumnName = "merchant_id")]
    public long MerchantId { get; private set; }

    [SugarColumn(ColumnName = "balance", ColumnDataType = "decimal(18,2)")]
    public decimal BalanceValue { get; private set; }

    [SugarColumn(IsIgnore = true)]
    public Money Balance
    {
        get => Money.Yuan(BalanceValue);
        private set => BalanceValue = value.Value;
    }

    [SugarColumn(ColumnName = "frozen_amount", ColumnDataType = "decimal(18,2)")]
    public decimal FrozenAmountValue { get; private set; }

    [SugarColumn(IsIgnore = true)]
    public Money FrozenAmount
    {
        get => Money.Yuan(FrozenAmountValue);
        private set => FrozenAmountValue = value.Value;
    }

    /// <summary>乐观锁版本号(由 AccountRepository.Update 自增,WHERE version=旧值 防并发覆盖)</summary>
    /// <remarks>
    /// 学习要点: version 的自增由基础设施层负责,领域层业务方法不自增
    ///   避免一个事务内多次调用业务方法导致 oldVersion 计算错误
    /// </remarks>
    [SugarColumn(ColumnName = "version")]
    public long Version { get; private set; }

    /// <summary>设置乐观锁版本号(仅供 AccountRepository.Update 使用,领域层不应调用)</summary>
    public void SetVersion(long version) => Version = version;

    [SugarColumn(ColumnName = "updated_at")]
    public DateTimeOffset UpdatedAt { get; private set; }

    // 学习要点: DomainEvents 由基类 AggregateRoot 统一声明 [SugarColumn(IsIgnore=true)]
    //   子类无需重复声明,直接继承使用即可

    // SqlSugar 5.x 要求 public 无参构造函数(见 Order.cs 注释说明)
    public Account() { }

    public static Account Create(long merchantId, Money initialBalance)
    {
        if (merchantId <= 0) throw new DomainException("商户ID无效");
        return new Account
        {
            MerchantId = merchantId,
            Balance = initialBalance,
            FrozenAmount = Money.Zero,
            Version = 0,
            UpdatedAt = DateTimeOffset.UtcNow
        };
    }

    /// <summary>入账(收入) —— 支付成功后商户余额增加</summary>
    public void Credit(Money amount)
    {
        if (amount.Value <= 0) throw new DomainException("入账金额必须大于0");
        Balance += amount;
        UpdatedAt = DateTimeOffset.UtcNow;
        // 学习要点: 此处不自增 Version,由 AccountRepository.Update 统一处理
    }

    /// <summary>出账(支出) —— 退款时余额扣减</summary>
    public void Debit(Money amount)
    {
        if (amount.Value <= 0) throw new DomainException("扣款金额必须大于0");
        if (Balance < amount) throw new DomainException($"余额不足: 当前 {Balance}, 需扣 {amount}");
        Balance -= amount;
        UpdatedAt = DateTimeOffset.UtcNow;
        // 学习要点: 此处不自增 Version,由 AccountRepository.Update 统一处理
    }

    /// <summary>冻结金额(退款预冻结) —— 从可用余额转入冻结</summary>
    public void Freeze(Money amount)
    {
        if (amount.Value <= 0) throw new DomainException("冻结金额必须大于0");
        if (Balance < amount) throw new DomainException("可用余额不足,无法冻结");
        Balance -= amount;
        FrozenAmount += amount;
        UpdatedAt = DateTimeOffset.UtcNow;
        // 学习要点: 此处不自增 Version,由 AccountRepository.Update 统一处理
    }

    /// <summary>解冻金额 —— 退款失败时,冻结金额转回可用</summary>
    public void Unfreeze(Money amount)
    {
        if (amount.Value <= 0) throw new DomainException("解冻金额必须大于0");
        if (FrozenAmount < amount) throw new DomainException("冻结金额不足");
        FrozenAmount -= amount;
        Balance += amount;
        UpdatedAt = DateTimeOffset.UtcNow;
        // 学习要点: 此处不自增 Version,由 AccountRepository.Update 统一处理
    }
}
