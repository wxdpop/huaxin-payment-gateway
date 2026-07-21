using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Payments;

/// <summary>
/// 支付记录实体 —— 记录每次支付请求与渠道响应
/// 学习要点:
///   1. (channel_code, channel_order_no) 唯一约束 = DB层幂等,防止重复入账
///   2. callback_raw 保存渠道原始回调报文,便于排查与对账
/// </summary>
[SugarTable("payment_records")]
public class PaymentRecord : Entity<long>
{
    [SugarColumn(ColumnName = "order_id")]
    public long OrderId { get; private set; }

    [SugarColumn(ColumnName = "channel_code", Length = 32, IsNullable = false)]
    public string ChannelCode { get; private set; } = string.Empty;

    [SugarColumn(ColumnName = "channel_order_no", Length = 64, IsNullable = false)]
    public string ChannelOrderNo { get; private set; } = string.Empty;

    [SugarColumn(ColumnName = "channel_trade_no", Length = 64, IsNullable = true)]
    public string? ChannelTradeNo { get; private set; }

    [SugarColumn(ColumnName = "amount", ColumnDataType = "decimal(18,2)")]
    public decimal AmountValue { get; private set; }

    [SugarColumn(IsIgnore = true)]
    public Money Amount
    {
        get => Money.Yuan(AmountValue);
        private set => AmountValue = value.Value;
    }

    [SugarColumn(ColumnName = "status", ColumnDataType = "smallint")]
    public PaymentStatus Status { get; private set; }

    [SugarColumn(ColumnName = "callback_raw", ColumnDataType = "text", IsNullable = true)]
    public string? CallbackRaw { get; private set; }

    [SugarColumn(ColumnName = "callback_at", IsNullable = true)]
    public DateTimeOffset? CallbackAt { get; private set; }

    [SugarColumn(ColumnName = "created_at")]
    public DateTimeOffset CreatedAt { get; private set; }

    // SqlSugar 5.x 要求 public 无参构造函数(见 Order.cs 注释说明)
    public PaymentRecord() { }

    /// <summary>
    /// 创建支付记录(发起支付时调用)
    /// </summary>
    public static PaymentRecord Create(
        long orderId, string channelCode, string channelOrderNo, Money amount)
    {
        if (orderId <= 0) throw new DomainException("订单ID无效");
        if (string.IsNullOrWhiteSpace(channelCode)) throw new DomainException("渠道编码不能为空");
        if (string.IsNullOrWhiteSpace(channelOrderNo)) throw new DomainException("渠道订单号不能为空");

        return new PaymentRecord
        {
            OrderId = orderId,
            ChannelCode = channelCode,
            ChannelOrderNo = channelOrderNo,
            Amount = amount,
            Status = PaymentStatus.Pending,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    /// <summary>标记支付成功(收到渠道成功回调)</summary>
    public void MarkSuccess(string? channelTradeNo, string callbackRaw)
    {
        if (Status == PaymentStatus.Success) return;  // 幂等
        Status = PaymentStatus.Success;
        ChannelTradeNo = channelTradeNo;
        CallbackRaw = callbackRaw;
        CallbackAt = DateTimeOffset.UtcNow;
    }

    /// <summary>标记支付失败</summary>
    public void MarkFailed(string callbackRaw)
    {
        if (Status == PaymentStatus.Failed) return;
        Status = PaymentStatus.Failed;
        CallbackRaw = callbackRaw;
        CallbackAt = DateTimeOffset.UtcNow;
    }
}
