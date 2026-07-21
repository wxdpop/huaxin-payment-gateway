namespace PaymentGateway.Domain.Refunds;

/// <summary>
/// 退款仓储接口 —— 领域层定义,基础设施层实现
/// 学习要点: 依赖倒置(DIP) —— 领域层定义接口,不依赖具体实现
/// </summary>
public interface IRefundRepository
{
    Task<RefundRecord?> GetByIdAsync(long id, CancellationToken ct = default);

    /// <summary>按退款单号查询(幂等校验用)</summary>
    Task<RefundRecord?> FindByRefundNoAsync(string refundNo, CancellationToken ct = default);

    /// <summary>按订单ID查询(检查订单是否已退款)</summary>
    Task<RefundRecord?> FindByOrderIdAsync(long orderId, CancellationToken ct = default);

    Task AddAsync(RefundRecord refund, CancellationToken ct = default);
    void Update(RefundRecord refund);
}
