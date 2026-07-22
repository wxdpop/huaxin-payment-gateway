// ============================================================================
// Saga 分布式事务演示项目
// ============================================================================
// 业务场景:
//   电商下单流程 = 创建订单 → 扣库存 → 支付
//   任何一步失败,需要补偿回滚前面已成功的步骤
//
// Saga 模式核心思想:
//   长事务拆成 N 个本地事务 T1,T2,T3...
//   每个本地事务 Ti 都有对应的补偿事务 Ci(反向操作)
//   失败时按相反顺序执行已成功步骤的补偿事务
//
//   示例:
//     T1: 创建订单    C1: 取消订单
//     T2: 扣库存      C2: 回库存
//     T3: 支付        C3: 退款
//
//   成功流程: T1 → T2 → T3
//   失败流程: T1 → T2 → T3失败 → C2 → C1
//
// Saga 分两种实现方式:
//   1. 编排式(Orchestration): 中央协调器指挥各服务(本示例采用)
//   2. 编排式(Choreography): 各服务订阅事件,自行响应(类似事件驱动)
//
// 项目结构:
//   Program.cs            - 入口,演示成功/失败两种场景
//   Models.cs             - 业务实体 + Saga 上下文
//   OrderService.cs       - 订单服务(创建/取消)
//   InventoryService.cs   - 库存服务(扣减/回滚)
//   PaymentService.cs     - 支付服务(支付/退款)
//   OrderSaga.cs          - Saga 协调器(编排整个事务)
// ============================================================================

namespace SagaDemo;

// ============================================================================
// 业务实体
// ============================================================================

/// <summary>
/// 订单实体
/// </summary>
public record Order(
    long Id,
    string ProductCode,    // 商品编码
    int Quantity,           // 购买数量
    decimal Amount,         // 订单金额
    OrderStatus Status);   // 订单状态

/// <summary>订单状态枚举</summary>
public enum OrderStatus
{
    Pending,    // 待支付
    Paid,       // 已支付
    Cancelled   // 已取消
}

/// <summary>
/// 库存实体
/// </summary>
public record Inventory(
    string ProductCode,
    int Available,    // 可用库存
    int Reserved);    // 预占库存(已扣减待支付)

/// <summary>
/// 支付记录实体
/// </summary>
public record Payment(
    long Id,
    long OrderId,
    decimal Amount,
    PaymentStatus Status);

/// <summary>支付状态枚举</summary>
public enum PaymentStatus
{
    Processing,  // 处理中
    Success,     // 成功
    Failed,      // 失败
    Refunded     // 已退款
}

// ============================================================================
// Saga 上下文 - 贯穿整个事务的状态载体
// ============================================================================

/// <summary>
/// Saga 上下文
/// 记录整个 Saga 的执行状态和中间结果
/// 补偿事务需要根据上下文中的"已成功步骤"决定回滚哪些
/// </summary>
public class SagaContext
{
    public long OrderId { get; set; }
    public string ProductCode { get; set; } = "";
    public int Quantity { get; set; }
    public decimal Amount { get; set; }

    // 每一步执行后保存的中间结果(供补偿事务使用)
    public Order? CreatedOrder { get; set; }       // T1 结果
    public bool InventoryReserved { get; set; }    // T2 结果
    public Payment? Payment { get; set; }           // T3 结果

    // 已执行的步骤列表(用于决定补偿顺序)
    private readonly List<string> _completedSteps = new();
    public IReadOnlyList<string> CompletedSteps => _completedSteps;

    public void MarkCompleted(string step)
    {
        _completedSteps.Add(step);
        Console.WriteLine($"  ✓ {step} 成功");
    }

    public void MarkFailed(string step, string reason)
    {
        Console.WriteLine($"  ✗ {step} 失败: {reason}");
    }
}

// ============================================================================
// Saga 执行结果
// ============================================================================

public record SagaResult(
    bool Success,
    string Message,
    SagaContext? Context = null);
