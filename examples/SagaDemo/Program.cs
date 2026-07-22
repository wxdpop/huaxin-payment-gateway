// ============================================================================
// Saga 分布式事务演示入口
// ============================================================================
// 演示两种典型场景:
//   场景 1: 全部成功 → T1 → T2 → T3
//   场景 2: 支付失败 → T1 → T2 → T3(失败) → C2 → C1
//
// 启动:
//   dotnet run --project examples/SagaDemo
// ============================================================================

using SagaDemo;

// 初始化三个服务(模拟三个独立微服务)
var orderSvc     = new OrderService();
var inventorySvc = new InventoryService();
var paymentSvc   = new PaymentService();

Console.WriteLine("============================================");
Console.WriteLine("  Saga 分布式事务演示");
Console.WriteLine("============================================");
Console.WriteLine("\n初始状态:");
PrintState(orderSvc, inventorySvc, paymentSvc);

// ============================================================================
// 场景 1: 全部成功
// ============================================================================
Console.WriteLine("\n\n============================================");
Console.WriteLine("  场景 1: 用户 1001(余额充足) 下单 SKU001");
Console.WriteLine("============================================");

// Saga 协调器 - 每次执行注入不同的 UserId
var saga1 = new OrderSaga(orderSvc, inventorySvc, paymentSvc, userIdProvider: () => 1001);
var result1 = await saga1.ExecuteAsync("SKU001", quantity: 5, amount: 500m);

Console.WriteLine($"\n结果: {(result1.Success ? "成功" : "失败")} - {result1.Message}");
PrintState(orderSvc, inventorySvc, paymentSvc);

// ============================================================================
// 场景 2: 支付失败 → 触发补偿
// ============================================================================
Console.WriteLine("\n\n============================================");
Console.WriteLine("  场景 2: 用户 1002(余额不足) 下单 SKU002");
Console.WriteLine("  预期:T1 成功 → T2 成功 → T3 失败 → C2 回滚 → C1 回滚");
Console.WriteLine("============================================");

var saga2 = new OrderSaga(orderSvc, inventorySvc, paymentSvc, userIdProvider: () => 1002);
var result2 = await saga2.ExecuteAsync("SKU002", quantity: 3, amount: 100m);

Console.WriteLine($"\n结果: {(result2.Success ? "成功" : "失败")} - {result2.Message}");
Console.WriteLine("  库存应该恢复原值,订单状态应为 Cancelled");
PrintState(orderSvc, inventorySvc, paymentSvc);

// ============================================================================
// 场景 3: 库存不足 → 触发补偿
// ============================================================================
Console.WriteLine("\n\n============================================");
Console.WriteLine("  场景 3: 库存不足");
Console.WriteLine("  预期:T1 成功 → T2 失败 → C1 回滚");
Console.WriteLine("============================================");

var saga3 = new OrderSaga(orderSvc, inventorySvc, paymentSvc, userIdProvider: () => 1001);
var result3 = await saga3.ExecuteAsync("SKU002", quantity: 9999, amount: 100m);

Console.WriteLine($"\n结果: {(result3.Success ? "成功" : "失败")} - {result3.Message}");
PrintState(orderSvc, inventorySvc, paymentSvc);

Console.WriteLine("\n演示结束");


// ============================================================================
// 调试输出函数
// ============================================================================
static void PrintState(OrderService os, InventoryService ivs, PaymentService ps)
{
    Console.WriteLine("\n--- 当前状态 ---");
    Console.WriteLine($"用户 1001 余额: {ps.GetBalance(1001)}");
    Console.WriteLine($"用户 1002 余额: {ps.GetBalance(1002)}");

    var inv1 = ivs.GetInventory("SKU001");
    var inv2 = ivs.GetInventory("SKU002");
    Console.WriteLine($"SKU001 库存: 可用={inv1?.Available}, 预占={inv1?.Reserved}");
    Console.WriteLine($"SKU002 库存: 可用={inv2?.Available}, 预占={inv2?.Reserved}");
    Console.WriteLine("----------------");
}
