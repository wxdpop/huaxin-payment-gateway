using PaymentGateway.Domain.Orders;
using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Application.Orders.Queries;

/// <summary>
/// 订单查询服务 —— CQRS 的 Query 部分(读操作)
/// 学习要点:
///   1. 读操作不走聚合根,直接通过仓储返回 DTO,避免加载完整聚合的开销
///   2. 查询服务与应用命令 Handler 分离,符合 CQRS"读写分离"原则
///   3. 后续可扩展为通过 SqlSugar Ado 或 Dapper 直接查 DB 提升性能
/// </summary>
public class OrderQueryService
{
    private readonly IOrderRepository _orderRepository;

    public OrderQueryService(IOrderRepository orderRepository)
    {
        _orderRepository = orderRepository;
    }

    /// <summary>按订单ID查询</summary>
    public async Task<OrderDetailDto> GetByIdAsync(long id, CancellationToken ct = default)
    {
        var order = await _orderRepository.GetByIdAsync(id, ct)
            ?? throw new BusinessException($"订单 {id} 不存在", "ORDER_NOT_FOUND");

        return ToDto(order);
    }

    /// <summary>按平台订单号查询</summary>
    public async Task<OrderDetailDto> GetByOrderNoAsync(string orderNo, CancellationToken ct = default)
    {
        var order = await _orderRepository.FindByOrderNoAsync(orderNo, ct)
            ?? throw new BusinessException($"订单 {orderNo} 不存在", "ORDER_NOT_FOUND");

        return ToDto(order);
    }

    private static OrderDetailDto ToDto(Order order) => new(
        order.Id,
        order.OrderNo,
        order.MerchantId,
        order.OutTradeNo,
        order.ChannelCode,
        order.ChannelOrderNo,
        order.Subject,
        order.Amount.Value,
        order.Status.ToString(),
        order.CreatedAt,
        order.PaidAt);
}
