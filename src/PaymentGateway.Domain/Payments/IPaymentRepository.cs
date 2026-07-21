namespace PaymentGateway.Domain.Payments;

/// <summary>
/// 支付记录仓储接口 —— 领域层定义,基础设施层实现
/// </summary>
public interface IPaymentRepository
{
    /// <summary>按渠道+渠道订单号查询(回调处理时用于幂等判断)</summary>
    Task<PaymentRecord?> FindByChannelOrderNoAsync(
        string channelCode, string channelOrderNo, CancellationToken ct = default);

    Task<PaymentRecord?> GetByIdAsync(long id, CancellationToken ct = default);

    Task AddAsync(PaymentRecord record, CancellationToken ct = default);
    void Update(PaymentRecord record);
}
