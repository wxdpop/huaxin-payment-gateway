namespace PaymentGateway.Domain.Payments;

/// <summary>
/// 支付记录状态
///   0 Pending 待支付(已创建支付记录,等待渠道回调)
///   1 Success 成功(收到渠道成功回调)
///   2 Failed  失败(收到渠道失败回调或超时)
/// </summary>
public enum PaymentStatus : short
{
    Pending = 0,
    Success = 1,
    Failed = 2
}
