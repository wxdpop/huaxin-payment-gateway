namespace PaymentGateway.Domain.Orders;

/// <summary>
/// 订单状态枚举 —— 状态机的合法状态
/// 学习要点: 用枚举集中定义状态,避免"魔法数字"(0/1/2 散落代码中)
///   0 待支付 → 用户下单成功,等待支付
///   1 支付中 → 已向渠道发起支付,等待回调
///   2 已支付 → 收到渠道成功回调,已记账
///   3 已退款 → 全额退款成功
///   4 已关闭 → 超时未支付/主动关闭
/// </summary>
public enum OrderStatus : short
{
    Pending = 0,
    Paying = 1,
    Paid = 2,
    Refunded = 3,
    Closed = 4
}

/// <summary>
/// 订单状态流转规则(状态机)
/// 学习要点: 将"允许的状态转换"集中定义,防止非法跳转
///   如 Paid 状态不能直接回到 Pending
/// </summary>
public static class OrderStatusExtensions
{
    /// <summary>判断是否允许从 from 转换到 to</summary>
    public static bool CanTransitTo(this OrderStatus from, OrderStatus to) => (from, to) switch
    {
        (OrderStatus.Pending, OrderStatus.Paying) => true,
        (OrderStatus.Paying, OrderStatus.Paid) => true,
        (OrderStatus.Paid, OrderStatus.Refunded) => true,
        (OrderStatus.Pending, OrderStatus.Closed) => true,
        (OrderStatus.Paying, OrderStatus.Closed) => true,
        _ => false
    };
}
