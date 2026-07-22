namespace SagaDemo;

// ============================================================================
// 订单服务 - 模拟独立微服务
// ============================================================================
// 职责:
//   T1: CreateOrderAsync - 创建订单(状态 = Pending)
//   C1: CancelOrderAsync - 取消订单(状态 = Cancelled)
//
// 学习要点:
//   1. 每个服务独立持有自己的"数据库"(这里用内存字典模拟)
//   2. 服务的正向操作和补偿操作成对出现
//   3. 补偿操作必须是幂等的(可能被重复调用)
// ============================================================================

public class OrderService
{
    // 模拟订单数据库(实际项目中是独立数据库)
    private readonly Dictionary<long, Order> _orders = new();
    private long _nextId = 1000;

    /// <summary>
    /// T1: 创建订单
    /// </summary>
    /// <param name="productCode">商品编码</param>
    /// <param name="quantity">数量</param>
    /// <param name="amount">金额</param>
    /// <returns>创建的订单</returns>
    public Task<Order> CreateOrderAsync(string productCode, int quantity, decimal amount)
    {
        var order = new Order(
            Id: Interlocked.Increment(ref _nextId),
            ProductCode: productCode,
            Quantity: quantity,
            Amount: amount,
            Status: OrderStatus.Pending);

        _orders[order.Id] = order;
        return Task.FromResult(order);
    }

    /// <summary>
    /// C1: 取消订单(补偿操作)
    /// 注意:
    ///   1. 幂等性 - 同一订单多次取消只生效一次
    ///   2. 只能取消 Pending 状态的订单
    /// </summary>
    public Task CancelOrderAsync(long orderId)
    {
        if (!_orders.TryGetValue(orderId, out var order))
        {
            // 幂等:订单不存在视为已取消
            Console.WriteLine($"  [OrderService] 订单 {orderId} 不存在,视为已取消");
            return Task.CompletedTask;
        }

        if (order.Status == OrderStatus.Cancelled)
        {
            // 幂等:已取消的订单不重复取消
            Console.WriteLine($"  [OrderService] 订单 {orderId} 已取消,跳过");
            return Task.CompletedTask;
        }

        // 执行补偿:状态改为 Cancelled
        _orders[orderId] = order with { Status = OrderStatus.Cancelled };
        return Task.CompletedTask;
    }

    /// <summary>
    /// 标记订单已支付(支付成功后由 Saga 调用)
    /// </summary>
    public Task MarkPaidAsync(long orderId)
    {
        if (_orders.TryGetValue(orderId, out var order))
            _orders[orderId] = order with { Status = OrderStatus.Paid };
        return Task.CompletedTask;
    }

    // 调试用:查询订单
    public Order? GetOrder(long id) =>
        _orders.TryGetValue(id, out var o) ? o : null;
}
