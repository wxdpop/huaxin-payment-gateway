using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Refunds;

/// <summary>
/// 退款记录实体 —— 记录退款申请与处理结果
/// 学习要点:
///   1. 退款必须基于已支付订单(状态机保证: Order.Paid → Refunded)
///   2. 退款流程: 冻结金额 → 调用渠道退款 → 成功扣减/失败解冻
///   3. refund_no 全局唯一,防止重复退款
/// </summary>
[SugarTable("refund_records")]
public class RefundRecord : Entity<long>
{
    [SugarColumn(ColumnName = "refund_no", Length = 32, IsNullable = false)]
    public string RefundNo { get; private set; } = string.Empty;

    [SugarColumn(ColumnName = "order_id")]
    public long OrderId { get; private set; }

    [SugarColumn(ColumnName = "merchant_id")]
    public long MerchantId { get; private set; }

    [SugarColumn(ColumnName = "amount", ColumnDataType = "decimal(18,2)")]
    public decimal AmountValue { get; private set; }

    [SugarColumn(IsIgnore = true)]
    public Money Amount
    {
        get => Money.Yuan(AmountValue);
        private set => AmountValue = value.Value;
    }

    [SugarColumn(ColumnName = "status", ColumnDataType = "smallint")]
    public RefundStatus Status { get; private set; }

    [SugarColumn(ColumnName = "channel_refund_no", Length = 64, IsNullable = true)]
    public string? ChannelRefundNo { get; private set; }

    [SugarColumn(ColumnName = "reason", Length = 256, IsNullable = true)]
    public string? Reason { get; private set; }

    [SugarColumn(ColumnName = "created_at")]
    public DateTimeOffset CreatedAt { get; private set; }

    [SugarColumn(ColumnName = "finished_at", IsNullable = true)]
    public DateTimeOffset? FinishedAt { get; private set; }

    // SqlSugar 5.x 要求 public 无参构造函数(见 Order.cs 注释说明)
    public RefundRecord() { }

    /// <summary>
    /// 创建退款记录(发起退款时调用)
    /// </summary>
    public static RefundRecord Create(
        long orderId, long merchantId, Money amount, string refundNo, string? reason = null)
    {
        if (orderId <= 0) throw new DomainException("订单ID无效");
        if (merchantId <= 0) throw new DomainException("商户ID无效");
        if (amount.Value <= 0) throw new DomainException("退款金额必须大于0");
        if (string.IsNullOrWhiteSpace(refundNo)) throw new DomainException("退款单号不能为空");

        return new RefundRecord
        {
            OrderId = orderId,
            MerchantId = merchantId,
            Amount = amount,
            RefundNo = refundNo,
            Reason = reason,
            Status = RefundStatus.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    /// <summary>标记退款成功(渠道退款成功回调)</summary>
    public void MarkRefunded(string? channelRefundNo)
    {
        if (Status == RefundStatus.Refunded) return;  // 幂等
        Status = RefundStatus.Refunded;
        ChannelRefundNo = channelRefundNo;
        FinishedAt = DateTimeOffset.UtcNow;
    }

    /// <summary>标记退款失败(渠道拒绝)</summary>
    public void MarkFailed()
    {
        if (Status == RefundStatus.Failed) return;
        Status = RefundStatus.Failed;
        FinishedAt = DateTimeOffset.UtcNow;
    }
}
