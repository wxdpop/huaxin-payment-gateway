using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Accounts;

/// <summary>
/// 交易类型 —— 账户流水的业务类型
/// </summary>
public enum TransactionType : short
{
    Credit = 1,    // 收入(支付入账)
    Debit = 2,      // 支出(退款扣款)
    Freeze = 3,     // 冻结(退款预冻结)
    Unfreeze = 4    // 解冻(退款失败回滚)
}

/// <summary>
/// 账户流水实体 —— 记录每一次余额变更(不可篡改,只增不改)
/// 学习要点:
///   1. 流水 immutable,UPDATE 禁止(资金审计要求)
///   2. balance_after 记录变更后余额,便于对账
///   3. biz_no 唯一约束 = 防重复记账(DB层幂等)
///   4. 账户余额应等于所有流水 SUM(amount with sign),定时对账校验
/// </summary>
[SugarTable("account_transactions")]
public class AccountTransaction : Entity<long>
{
    [SugarColumn(ColumnName = "account_id")]
    public long AccountId { get; private set; }

    [SugarColumn(ColumnName = "order_id", IsNullable = true)]
    public long? OrderId { get; private set; }

    [SugarColumn(ColumnName = "tx_type", ColumnDataType = "smallint")]
    public TransactionType TxType { get; private set; }

    [SugarColumn(ColumnName = "amount", ColumnDataType = "decimal(18,2)")]
    public decimal AmountValue { get; private set; }

    [SugarColumn(IsIgnore = true)]
    public Money Amount
    {
        get => Money.Yuan(AmountValue);
        private set => AmountValue = value.Value;
    }

    /// <summary>变更后余额(对账用,反推验证)</summary>
    [SugarColumn(ColumnName = "balance_after", ColumnDataType = "decimal(18,2)")]
    public decimal BalanceAfterValue { get; private set; }

    [SugarColumn(IsIgnore = true)]
    public Money BalanceAfter
    {
        get => Money.Yuan(BalanceAfterValue);
        private set => BalanceAfterValue = value.Value;
    }

    /// <summary>业务单号(如订单号),唯一约束防重复记账</summary>
    [SugarColumn(ColumnName = "biz_no", Length = 64, IsNullable = false)]
    public string BizNo { get; private set; } = string.Empty;

    [SugarColumn(ColumnName = "remark", Length = 256, IsNullable = true)]
    public string? Remark { get; private set; }

    [SugarColumn(ColumnName = "created_at")]
    public DateTimeOffset CreatedAt { get; private set; }

    // SqlSugar 5.x 要求 public 无参构造函数(见 Order.cs 注释说明)
    public AccountTransaction() { }

    public static AccountTransaction Create(
        long accountId,
        long? orderId,
        TransactionType txType,
        Money amount,
        Money balanceAfter,
        string bizNo,
        string? remark = null)
    {
        if (accountId <= 0) throw new DomainException("账户ID无效");
        if (amount.Value <= 0) throw new DomainException("金额必须大于0");
        if (string.IsNullOrWhiteSpace(bizNo)) throw new DomainException("业务单号不能为空");

        return new AccountTransaction
        {
            AccountId = accountId,
            OrderId = orderId,
            TxType = txType,
            Amount = amount,
            BalanceAfter = balanceAfter,
            BizNo = bizNo,
            Remark = remark,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }
}
