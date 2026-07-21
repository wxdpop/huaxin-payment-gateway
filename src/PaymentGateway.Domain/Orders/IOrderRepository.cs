namespace PaymentGateway.Domain.Orders;

/// <summary>
/// 订单仓储接口 —— 领域层定义,基础设施层实现
/// 学习要点: 依赖倒置(DIP) —— 领域层定义接口,不依赖具体实现
///   这样领域逻辑可独立测试(用 Mock 仓储),且可替换不同存储
/// </summary>
public interface IOrderRepository
{
    Task<Order?> GetByIdAsync(long id, CancellationToken ct = default);
    Task<Order?> FindByOrderNoAsync(string orderNo, CancellationToken ct = default);

    /// <summary>按渠道订单号查询(回调处理时用)</summary>
    Task<Order?> FindByChannelOrderNoAsync(string channelOrderNo, CancellationToken ct = default);

    Task AddAsync(Order order, CancellationToken ct = default);
    void Update(Order order);
}
