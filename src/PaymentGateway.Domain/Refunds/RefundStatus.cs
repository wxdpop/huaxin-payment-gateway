namespace PaymentGateway.Domain.Refunds;

/// <summary>
/// 退款状态枚举
///   0 Pending  待退款(已申请,等待渠道处理)
///   1 Refunded 已退款(渠道退款成功,资金已扣减)
///   2 Failed   退款失败(渠道拒绝,冻结金额解冻)
/// </summary>
public enum RefundStatus : short
{
    Pending = 0,
    Refunded = 1,
    Failed = 2
}
