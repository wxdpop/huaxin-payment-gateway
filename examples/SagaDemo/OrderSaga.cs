namespace SagaDemo;

// ============================================================================
// Saga 协调器(Orchestrator)
// ============================================================================
// 核心职责:
//   1. 按顺序执行各服务的正向操作(T1 → T2 → T3)
//   2. 任何一步失败,按相反顺序执行已成功步骤的补偿事务(Ci)
//   3. 通过 SagaContext 贯穿整个事务状态
//
// 学习要点:
//   1. 编排式 Saga:中央协调器是关键,所有服务调用都经过它
//   2. 补偿顺序:成功顺序是 T1,T2,T3,补偿顺序必须是 C3,C2,C1
//   3. 补偿必须幂等:网络重试或重复调用不能产生副作用
//   4. 补偿也可能失败:需要重试机制 + 人工干预告警
// ============================================================================

public class OrderSaga
{
    private readonly OrderService _orderSvc;
    private readonly InventoryService _inventorySvc;
    private readonly PaymentService _paymentSvc;

    // 用 Func<long> 注入 UserId,方便演示成功/失败两种场景
    private readonly Func<long> _userIdProvider;

    public OrderSaga(
        OrderService orderSvc,
        InventoryService inventorySvc,
        PaymentService paymentSvc,
        Func<long> userIdProvider)
    {
        _orderSvc = orderSvc;
        _inventorySvc = inventorySvc;
        _paymentSvc = paymentSvc;
        _userIdProvider = userIdProvider;
    }

    /// <summary>
    /// 执行 Saga 完整流程
    /// 成功路径: T1(创建订单) → T2(扣库存) → T3(支付)
    /// 失败路径: 任意步骤失败 → 补偿已成功步骤 → 返回失败
    /// </summary>
    public async Task<SagaResult> ExecuteAsync(string productCode, int quantity, decimal amount)
    {
        var ctx = new SagaContext
        {
            ProductCode = productCode,
            Quantity = quantity,
            Amount = amount
        };

        Console.WriteLine($"\n[Saga] 开始执行,商品 {productCode},数量 {quantity},金额 {amount}");
        Console.WriteLine("[Saga] 正向流程开始");

        // ========================================================
        // 步骤 1: T1 创建订单
        // ========================================================
        try
        {
            var order = await _orderSvc.CreateOrderAsync(productCode, quantity, amount);
            ctx.OrderId = order.Id;
            ctx.CreatedOrder = order;
            ctx.MarkCompleted($"T1 创建订单 (orderId={order.Id})");
        }
        catch (Exception ex)
        {
            ctx.MarkFailed("T1 创建订单", ex.Message);
            // T1 失败 → 无需补偿,直接返回
            return new SagaResult(false, $"T1 失败: {ex.Message}", ctx);
        }

        // ========================================================
        // 步骤 2: T2 扣减库存
        // ========================================================
        try
        {
            var reservationId = $"order-{ctx.OrderId}";  // 用订单号作预占 ID,保证幂等
            await _inventorySvc.ReserveAsync(productCode, quantity, reservationId);
            ctx.InventoryReserved = true;
            ctx.MarkCompleted($"T2 扣减库存 (reservation={reservationId})");
        }
        catch (Exception ex)
        {
            ctx.MarkFailed("T2 扣减库存", ex.Message);
            // T2 失败 → 补偿 T1(取消订单)
            await CompensateAsync(ctx);
            return new SagaResult(false, $"T2 失败: {ex.Message}", ctx);
        }

        // ========================================================
        // 步骤 3: T3 支付
        // ========================================================
        try
        {
            var userId = _userIdProvider();
            var payment = await _paymentSvc.PayAsync(ctx.OrderId, amount, userId);
            ctx.Payment = payment;
            ctx.MarkCompleted($"T3 支付 (paymentId={payment.Id})");

            // 支付成功后,确认库存扣减(预占 → 真扣)
            await _inventorySvc.CommitAsync(productCode, quantity);
            await _orderSvc.MarkPaidAsync(ctx.OrderId);
            Console.WriteLine("  ✓ 库存确认扣减,订单标记为已支付");
        }
        catch (Exception ex)
        {
            ctx.MarkFailed("T3 支付", ex.Message);
            // T3 失败 → 补偿 T2 + T1
            await CompensateAsync(ctx);
            return new SagaResult(false, $"T3 失败: {ex.Message}", ctx);
        }

        Console.WriteLine("[Saga] ✓ 全部成功完成");
        return new SagaResult(true, "Saga 全部成功", ctx);
    }

    /// <summary>
    /// 补偿事务 - 按相反顺序执行已成功步骤的补偿
    /// 关键: 永远按 T3 → T2 → T1 的反向顺序,跳过未执行的步骤
    /// </summary>
    private async Task CompensateAsync(SagaContext ctx)
    {
        Console.WriteLine("[Saga] ⚠ 触发补偿,按相反顺序回滚");

        // C3: 退款(仅当 T3 已成功)
        // 注意:T3 失败时不会执行 C3(因为支付根本没成功)
        if (ctx.Payment is not null)
        {
            try
            {
                await _paymentSvc.RefundAsync(ctx.Payment.Id);
                Console.WriteLine("  ✓ C3 退款成功");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ✗ C3 退款失败: {ex.Message} (需人工介入)");
            }
        }

        // C2: 释放预占库存(仅当 T2 已成功)
        if (ctx.InventoryReserved)
        {
            try
            {
                var reservationId = $"order-{ctx.OrderId}";
                await _inventorySvc.ReleaseAsync(ctx.ProductCode, ctx.Quantity, reservationId);
                Console.WriteLine("  ✓ C2 释放库存成功");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ✗ C2 释放库存失败: {ex.Message}");
            }
        }

        // C1: 取消订单(仅当 T1 已成功)
        if (ctx.CreatedOrder is not null)
        {
            try
            {
                await _orderSvc.CancelOrderAsync(ctx.OrderId);
                Console.WriteLine("  ✓ C1 取消订单成功");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ✗ C1 取消订单失败: {ex.Message}");
            }
        }

        Console.WriteLine("[Saga] 补偿完成");
    }
}
