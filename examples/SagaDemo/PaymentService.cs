namespace SagaDemo;

// ============================================================================
// 支付服务 - 模拟独立微服务
// ============================================================================
// 职责:
//   T3: PayAsync - 执行支付
//   C3: RefundAsync - 退款(补偿操作)
//
// 学习要点:
//   1. 支付可能因余额不足/渠道故障失败 → 触发 Saga 补偿
//   2. 退款必须异步:渠道退款通常 T+1 到账,Saga 只记录退款请求
//   3. 支付服务的失败是 Saga 触发补偿的典型场景
// ============================================================================

public class PaymentException : Exception
{
    public PaymentException(string msg) : base(msg) { }
}

public class PaymentService
{
    // 模拟支付记录数据库
    private readonly Dictionary<long, Payment> _payments = new();
    private long _nextId = 5000;

    // 模拟用户余额(用于演示支付失败场景)
    private readonly Dictionary<long, decimal> _userBalances = new()
    {
        [1001] = 10000m,   // 余额充足,支付成功
        [1002] = 50m       // 余额不足,支付失败(用于演示 Saga 补偿)
    };

    /// <summary>
    /// T3: 执行支付
    /// </summary>
    /// <param name="orderId">订单 ID</param>
    /// <param name="amount">支付金额</param>
    /// <param name="userId">用户 ID(决定余额是否充足)</param>
    public Task<Payment> PayAsync(long orderId, decimal amount, long userId)
    {
        // 检查余额
        if (!_userBalances.TryGetValue(userId, out var balance) || balance < amount)
            throw new PaymentException(
                $"支付失败:用户 {userId} 余额 {balance},需要 {amount}");

        // 扣减余额
        _userBalances[userId] = balance - amount;

        // 创建支付记录
        var payment = new Payment(
            Id: Interlocked.Increment(ref _nextId),
            OrderId: orderId,
            Amount: amount,
            Status: PaymentStatus.Success);

        _payments[payment.Id] = payment;
        return Task.FromResult(payment);
    }

    /// <summary>
    /// C3: 退款(补偿操作)
    /// </summary>
    /// <param name="paymentId">支付 ID</param>
    public Task RefundAsync(long paymentId)
    {
        if (!_payments.TryGetValue(paymentId, out var payment))
        {
            Console.WriteLine($"  [PaymentService] 支付 {paymentId} 不存在,跳过退款");
            return Task.CompletedTask;
        }

        if (payment.Status == PaymentStatus.Refunded)
        {
            Console.WriteLine($"  [PaymentService] 支付 {paymentId} 已退款,跳过");
            return Task.CompletedTask;
        }

        // 退还用户余额
        if (_userBalances.ContainsKey(payment.OrderId))
            _userBalances[payment.OrderId] += payment.Amount;

        // 标记为已退款
        _payments[paymentId] = payment with { Status = PaymentStatus.Refunded };
        return Task.CompletedTask;
    }

    // 调试用
    public Payment? GetPayment(long id) =>
        _payments.TryGetValue(id, out var p) ? p : null;

    public decimal GetBalance(long userId) =>
        _userBalances.TryGetValue(userId, out var b) ? b : 0;
}
