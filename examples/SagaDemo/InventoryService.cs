namespace SagaDemo;

// ============================================================================
// 库存服务 - 模拟独立微服务
// ============================================================================
// 职责:
//   T2: ReserveAsync - 预占库存(扣减可用,增加预占)
//   C2: ReleaseAsync - 释放预占(扣减预占,增加可用)
//
// 学习要点:
//   1. 预占 vs 真扣:实际电商中下单时只预占,支付成功后才真扣
//   2. 库存不足要抛异常,Saga 捕获后触发补偿
//   3. 释放操作幂等:同一笔预占多次释放只生效一次
// ============================================================================

public class InventoryException : Exception
{
    public InventoryException(string msg) : base(msg) { }
}

public class InventoryService
{
    // 模拟库存数据库
    private readonly Dictionary<string, Inventory> _inventories = new();
    // 记录已释放的预占(用于幂等性)
    private readonly HashSet<string> _releasedReservations = new();

    public InventoryService()
    {
        // 初始化库存
        _inventories["SKU001"] = new Inventory("SKU001", Available: 100, Reserved: 0);
        _inventories["SKU002"] = new Inventory("SKU002", Available: 50, Reserved: 0);
    }

    /// <summary>
    /// T2: 预占库存
    /// </summary>
    /// <param name="productCode">商品编码</param>
    /// <param name="quantity">数量</param>
    /// <param name="reservationId">预占 ID(用于幂等)</param>
    public Task ReserveAsync(string productCode, int quantity, string reservationId)
    {
        // 幂等检查:同一 reservationId 已处理过直接返回
        if (_releasedReservations.Contains(reservationId))
            throw new InventoryException($"预占 {reservationId} 已被释放,不能再次预占");

        if (!_inventories.TryGetValue(productCode, out var inv))
            throw new InventoryException($"商品 {productCode} 不存在");

        if (inv.Available < quantity)
            throw new InventoryException(
                $"库存不足:可用 {inv.Available},请求 {quantity}");

        // 扣减可用,增加预占
        _inventories[productCode] = inv with
        {
            Available = inv.Available - quantity,
            Reserved = inv.Reserved + quantity
        };
        return Task.CompletedTask;
    }

    /// <summary>
    /// C2: 释放预占(补偿操作)
    /// 幂等:同一 reservationId 多次释放只生效一次
    /// </summary>
    public Task ReleaseAsync(string productCode, int quantity, string reservationId)
    {
        // 幂等检查
        if (!_releasedReservations.Add(reservationId))
        {
            Console.WriteLine($"  [InventoryService] 预占 {reservationId} 已释放,跳过");
            return Task.CompletedTask;
        }

        if (!_inventories.TryGetValue(productCode, out var inv))
            return Task.CompletedTask;

        // 扣减预占,增加可用
        _inventories[productCode] = inv with
        {
            Available = inv.Available + quantity,
            Reserved = Math.Max(0, inv.Reserved - quantity)
        };
        return Task.CompletedTask;
    }

    /// <summary>
    /// 确认扣减(支付成功后调用,把预占转为真实扣减)
    /// </summary>
    public Task CommitAsync(string productCode, int quantity)
    {
        if (!_inventories.TryGetValue(productCode, out var inv))
            return Task.CompletedTask;

        _inventories[productCode] = inv with
        {
            Reserved = Math.Max(0, inv.Reserved - quantity)
        };
        return Task.CompletedTask;
    }

    // 调试用
    public Inventory? GetInventory(string code) =>
        _inventories.TryGetValue(code, out var i) ? i : null;
}
