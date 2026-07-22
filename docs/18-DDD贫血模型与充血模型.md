# DDD 贫血模型 vs 充血模型详解

> 面向 .NET 工程师的最实用 DDD 模型对比，含本项目中实际代码示例。

---

## 1. 两种模型是什么

### 1.1 贫血模型（Anemic Model）

**定义**：实体只有数据（属性 + getter/setter），业务逻辑全部放在 Service 层。

**典型代码**：
```csharp
// 实体 = 纯数据容器
public class Order
{
    public long Id { get; set; }
    public string OrderNo { get; set; }
    public decimal Amount { get; set; }
    public OrderStatus Status { get; set; }   // ← 外部可随便 set
    public DateTime CreatedAt { get; set; }
}

// 业务逻辑全在 Service
public class OrderService
{
    public void Pay(Order order)
    {
        if (order.Status != OrderStatus.Pending)
            throw new Exception("订单状态不允许支付");
        order.Status = OrderStatus.Paid;        // ← Service 直接改属性
        order.PaidAt = DateTime.Now;
    }
}
```

### 1.2 充血模型（Rich Model）

**定义**：实体既包含数据，也封装业务行为（方法），外部不能直接改状态，必须通过方法。

**典型代码**（本项目实践）：
```csharp
// 实体 = 数据 + 行为
public class Order : AggregateRoot<long>
{
    public string OrderNo { get; private set; }    // ← private set
    public decimal Amount { get; private set; }
    public OrderStatus Status { get; private set; }

    // 行为方法:封装状态变更 + 校验
    public void MarkAsPaid()
    {
        EnsureCanTransit(OrderStatus.Paid);        // ← 内部校验状态机
        Status = OrderStatus.Paid;
        PaidAt = DateTimeOffset.UtcNow;
        AddDomainEvent(new OrderPaidEvent(...));  // ← 产生领域事件
    }
}
```

### 1.3 一句话对比

> **贫血模型**：实体是"哑数据"，Service 是"胖逻辑"。
> **充血模型**：实体是"会做事的对象"，Service 只负责编排。

---

## 2. 核心差异对比

| 维度 | 贫血模型 | 充血模型 |
|------|---------|---------|
| **属性可见性** | public get/set | public get / private set |
| **业务逻辑位置** | Service 层 | 实体内部 |
| **状态变更方式** | 直接 `order.Status = ...` | 必须 `order.MarkAsPaid()` |
| **校验逻辑** | Service 内 if 判断 | 实体内 EnsureCanXxx 方法 |
| **领域事件** | Service 内发布 | 实体内 AddDomainEvent |
| **不变量保护** | 依赖 Service 自觉 | 实体强制保证 |
| **可测试性** | Service 测试 | 实体单元测试（轻量） |
| **代码量** | 实体少 + Service 多 | 实体多 + Service 简洁 |
| **学习成本** | 低（看属性即懂） | 中（要看方法语义） |
| **DDD 纯度** | 反模式 | 推荐 |

---

## 3. 本项目实践（充血模型）

### 3.1 Order 聚合根（充血）

本项目 [Order.cs](file:///c:/Users/ZJN/Desktop/jl/project/src/PaymentGateway.Domain/Orders/Order.cs) 完整实践了充血模型：

```csharp
[SugarTable("orders")]
public class Order : AggregateRoot<long>
{
    // 1. 属性全部 private set,外部不能直接改
    public string OrderNo { get; private set; } = string.Empty;
    public OrderStatus Status { get; private set; }

    // 2. 工厂方法:保证创建时不变量成立
    public static Order Create(long merchantId, string orderNo, ..., Money amount, ...)
    {
        if (merchantId <= 0) throw new DomainException("商户ID无效");
        if (amount.Value <= 0) throw new DomainException("订单金额必须大于0");
        // ...
        var order = new Order { ... };
        order.AddDomainEvent(new OrderCreatedEvent(...));
        return order;
    }

    // 3. 行为方法:封装状态机变更 + 校验
    public void MarkAsPaying(string channelCode, string channelOrderNo)
    {
        EnsureCanTransit(OrderStatus.Paying);      // ← 内部校验
        ChannelCode = channelCode ?? throw new DomainException("...");
        Status = OrderStatus.Paying;
    }

    public void MarkAsPaid()
    {
        EnsureCanTransit(OrderStatus.Paid);
        Status = OrderStatus.Paid;
        PaidAt = DateTimeOffset.UtcNow;
        AddDomainEvent(new OrderPaidEvent(...));   // ← 领域事件
    }

    // 4. 私有校验方法
    private void EnsureCanTransit(OrderStatus target)
    {
        if (!Status.CanTransitTo(target))
            throw new DomainException($"订单状态 {Status} 不允许转换为 {target}");
    }
}
```

### 3.2 应用层 Service（编排）

充血模型下 Service 变薄，只负责"事务 + 编排"：

```csharp
public class PayOrderService : IPayOrderService
{
    private readonly IOrderRepository _orderRepo;
    private readonly IUnitOfWork _uow;

    public async Task<PayOrderResult> PayAsync(PayOrderCommand cmd, CancellationToken ct)
    {
        // 1. 加载聚合根
        var order = await _orderRepo.GetByIdAsync(cmd.OrderId, ct)
            ?? throw new DomainException("订单不存在");

        // 2. 调用实体的行为方法(核心业务在实体内)
        order.MarkAsPaying(channelCode, channelOrderNo);

        // 3. 持久化(事务)
        await _orderRepo.UpdateAsync(order, ct);
        await _uow.SaveChangesAsync(ct);

        // 4. 返回结果
        return new PayOrderResult(order.OrderNo, ...);
    }
}
```

**对比**：贫血模型下，`PayAsync` 内部要写 30 行校验 + 状态变更 + 事件发布；充血模型下，业务集中在 `order.MarkAsPaying()` 一行。

---

## 4. 贫血 vs 充血的代码对比

### 4.1 同一业务"订单支付"两种写法

#### 贫血模型版本

```csharp
// 实体
public class Order
{
    public long Id { get; set; }
    public OrderStatus Status { get; set; }       // ← public set,谁都能改
    public string? ChannelCode { get; set; }
    public DateTime? PaidAt { get; set; }
}

// Service 写满业务逻辑
public class OrderService
{
    private readonly IOrderRepository _repo;

    public async Task PayAsync(long orderId, string channelCode, string channelOrderNo)
    {
        var order = await _repo.GetByIdAsync(orderId);
        if (order == null) throw new Exception("订单不存在");

        // 校验状态
        if (order.Status != OrderStatus.Pending)
            throw new Exception($"订单状态 {order.Status} 不允许支付");

        // 校验参数
        if (string.IsNullOrEmpty(channelCode))
            throw new Exception("渠道编码不能为空");

        // 改属性
        order.Status = OrderStatus.Paying;
        order.ChannelCode = channelCode;
        order.ChannelOrderNo = channelOrderNo;

        await _repo.UpdateAsync(order);
    }

    public async Task MarkAsPaidAsync(long orderId)
    {
        var order = await _repo.GetByIdAsync(orderId);
        if (order.Status != OrderStatus.Paying)
            throw new Exception("订单状态不允许标记为已支付");

        order.Status = OrderStatus.Paid;
        order.PaidAt = DateTime.UtcNow;

        // 事件发布也散落各 Service
        await _eventBus.PublishAsync(new OrderPaidEvent(order.Id, ...));

        await _repo.UpdateAsync(order);
    }
}
```

**问题**：
- ❌ 任何代码都能 `order.Status = OrderStatus.Paid` 绕过校验
- ❌ 状态机校验散落在多个 Service 方法中，重复
- ❌ 事件发布逻辑分散，容易漏发

#### 充血模型版本（本项目实际代码）

```csharp
// 实体
public class Order : AggregateRoot<long>
{
    public OrderStatus Status { get; private set; }   // ← private set
    public string? ChannelCode { get; private set; }

    public void MarkAsPaying(string channelCode, string channelOrderNo)
    {
        EnsureCanTransit(OrderStatus.Paying);          // ← 内部校验
        ChannelCode = channelCode ?? throw new DomainException("...");
        Status = OrderStatus.Paying;
    }

    public void MarkAsPaid()
    {
        EnsureCanTransit(OrderStatus.Paid);
        Status = OrderStatus.Paid;
        PaidAt = DateTimeOffset.UtcNow;
        AddDomainEvent(new OrderPaidEvent(...));       // ← 实体内发布事件
    }
}

// Service 简化为编排
public class PayOrderService
{
    public async Task PayAsync(long orderId, string channelCode, string channelOrderNo)
    {
        var order = await _repo.GetByIdAsync(orderId);
        order.MarkAsPaying(channelCode, channelOrderNo);  // ← 一行搞定
        await _repo.UpdateAsync(order);
    }
}
```

**优势**：
- ✅ 状态变更只能通过 `MarkAsPaying/MarkAsPaid`，无法绕过校验
- ✅ 状态机校验集中在实体内，不重复
- ✅ 领域事件在变更同时发布，不会漏发

---

## 5. 何时用贫血 / 充血

### 5.1 选型决策

| 场景 | 推荐模型 | 理由 |
|------|---------|------|
| **CRUD 后台管理** | 贫血 | 业务简单，无状态机，贫血更快 |
| **表单提交系统** | 贫血 | 纯数据映射，无复杂规则 |
| **微服务简单 DTO** | 贫血 | 仅做传输，无业务 |
| **支付/交易系统** | 充血 | 资金安全必须强制不变量 |
| **订单/库存系统** | 充血 | 状态机复杂，必须封装 |
| **DDD 战术设计** | 充血 | 聚合根 + 实体 + 值对象 |
| **业务规则会演进** | 充血 | 修改集中在实体，不扩散 |

### 5.2 不要教条

**贫血不是反模式**，是工具选择：
- ✅ 简单 CRUD 用贫血，开发快、维护成本低
- ✅ 复杂业务用充血，保证不变量、可测试性

**反模式是"业务复杂却用贫血"**：
- 多个 Service 改同一实体的同一属性 → 状态散落，难维护
- 校验逻辑复制粘贴 → 修改漏一处就出 Bug

---

## 6. 充血模型的关键模式

### 6.1 工厂方法（Create）

```csharp
public static Order Create(long merchantId, string orderNo, Money amount, ...)
{
    // 创建时校验不变量
    if (merchantId <= 0) throw new DomainException("商户ID无效");
    if (amount.Value <= 0) throw new DomainException("金额必须大于0");

    var order = new Order { ... };
    order.AddDomainEvent(new OrderCreatedEvent(...));
    return order;
}
```

**为什么不用构造函数？**
- 构造函数不能 async
- 工厂方法可缓存（相同参数返回同一实例）
- 工厂方法语义更清晰（`Order.Create` vs `new Order`）

### 6.2 private set + 行为方法

```csharp
public OrderStatus Status { get; private set; }   // ← 外部只读

// 变更必须走方法
public void MarkAsPaid()
{
    EnsureCanTransit(OrderStatus.Paid);
    Status = OrderStatus.Paid;
}
```

**关键**：编译期保证不变量，外部无法绕过。

### 6.3 状态机校验

```csharp
private void EnsureCanTransit(OrderStatus target)
{
    if (!Status.CanTransitTo(target))
        throw new DomainException($"订单状态 {Status} 不允许转换为 {target}");
}
```

**状态转换表集中在 OrderStatus 枚举**：

```csharp
public enum OrderStatus
{
    Pending, Paying, Paid, Refunded, Closed
}

public static class OrderStatusExtensions
{
    private static readonly Dictionary<OrderStatus, OrderStatus[]> _transitions = new()
    {
        [OrderStatus.Pending]  = new[] { OrderStatus.Paying, OrderStatus.Closed },
        [OrderStatus.Paying]   = new[] { OrderStatus.Paid, OrderStatus.Closed },
        [OrderStatus.Paid]     = new[] { OrderStatus.Refunded },
        [OrderStatus.Refunded] = Array.Empty<OrderStatus>(),
        [OrderStatus.Closed]   = Array.Empty<OrderStatus>(),
    };

    public static bool CanTransitTo(this OrderStatus from, OrderStatus to)
        => _transitions.TryGetValue(from, out var targets) && targets.Contains(to);
}
```

### 6.4 领域事件

```csharp
public abstract class AggregateRoot<TKey>
{
    private readonly List<IDomainEvent> _events = new();
    public IReadOnlyList<IDomainEvent> DomainEvents => _events;

    protected void AddDomainEvent(IDomainEvent evt) => _events.Add(evt);
    public void ClearDomainEvents() => _events.Clear();
}
```

**业务方法内发事件**：
```csharp
public void MarkAsPaid()
{
    // ...
    AddDomainEvent(new OrderPaidEvent(Id, OrderNo, ...));   // ← 同事务内
}
```

**应用层在事务提交后统一发布**：
```csharp
public async Task PayAsync(...)
{
    order.MarkAsPaid();
    await _repo.UpdateAsync(order);
    await _uow.SaveChangesAsync(ct);

    // 事务提交后发布事件
    foreach (var evt in order.DomainEvents)
        await _eventBus.PublishAsync(evt);
    order.ClearDomainEvents();
}
```

---

## 7. ORM 与充血模型的冲突

### 7.1 常见冲突

| 冲突 | 贫血 | 充血 |
|------|------|------|
| **构造函数** | public 无参 | 倾向 private（强制工厂） |
| **属性 set** | public | private |
| **懒加载** | ORM 直接 set | 需要 `internal set` |
| **反序列化** | 直接 set 属性 | 需要反射或构造函数 |

### 7.2 本项目折中方案

[Order.cs#L80](file:///c:/Users/ZJN/Desktop/jl/project/src/PaymentGateway.Domain/Orders/Order.cs#L80) 注释明确写了取舍：

```csharp
// SqlSugar 5.x 要求 public 无参构造函数
// 严格 DDD 倾向 protected/private 构造函数(强制走工厂方法),
// 但 SqlSugar 等主流 ORM 要求 public,这里遵循 ORM 约定
public Order() { }
```

**取舍**：
- ORM 需要 → 暴露 public 无参构造函数
- 业务校验 → 通过工厂方法 `Create` 集中处理
- 状态变更 → 通过 `MarkAsXxx` 方法封装

### 7.3 EF Core 的支持

EF Core 2.0+ 支持更纯的充血模型：
```csharp
public class Order
{
    public int Id { get; private set; }            // private set OK
    public string Status { get; private set; } = "Pending";

    private Order() { }                            // private 构造函数 OK

    public static Order Create(...) { ... }         // 工厂方法
}
// EF Core 通过反射访问 private 成员,无需 public set
```

---

## 8. 测试对比

### 8.1 贫血模型的测试

```csharp
[Fact]
public void Pay_Order_Pending_Status_Changes_To_Paying()
{
    // Arrange
    var order = new Order { Status = OrderStatus.Pending };  // ← 随便造数据
    var svc = new OrderService(mockRepo);

    // Act
    svc.Pay(order, "wechat", "ch001");

    // Assert
    Assert.Equal(OrderStatus.Paying, order.Status);
}

[Fact]
public void Pay_Order_Paid_Throws()
{
    var order = new Order { Status = OrderStatus.Paid };   // ← 直接构造非法状态
    var svc = new OrderService(mockRepo);

    Assert.Throws<Exception>(() => svc.Pay(order, "wechat", "ch001"));
}
```

**问题**：`new Order { Status = OrderStatus.Paid }` 可构造出"已支付但未扣款"的非法状态。

### 8.2 充血模型的测试

```csharp
[Fact]
public void MarkAsPaid_From_Paying_Changes_To_Paid()
{
    // Arrange - 通过工厂方法创建合法状态
    var order = Order.Create(merchantId: 1, orderNo: "O001", ...);
    order.MarkAsPaying("wechat", "ch001");

    // Act
    order.MarkAsPaid();

    // Assert
    Assert.Equal(OrderStatus.Paid, order.Status);
    Assert.NotNull(order.PaidAt);
    Assert.Single(order.DomainEvents, e => e is OrderPaidEvent);
}

[Fact]
public void MarkAsPaid_From_Pending_Throws()
{
    var order = Order.Create(1, "O001", Money.Yuan(100), "test");

    Assert.Throws<DomainException>(() => order.MarkAsPaid());
}
```

**优势**：
- ✅ 测试只测实体，无需 Mock Repository
- ✅ 单元测试毫秒级（不依赖 DB）
- ✅ 工厂方法保证测试起点合法

---

## 9. 常见误区

### 9.1 误区 1：充血 = 把所有逻辑塞进实体

**错误**：
```csharp
public class Order
{
    public void Pay(IPaymentGateway gateway, IAccountRepository accountRepo)
    {
        // ❌ 不要在实体内做基础设施操作
        gateway.Charge(Amount);
        accountRepo.Deduct(MerchantId, Amount);
        Status = OrderStatus.Paid;
    }
}
```

**正确**：实体只做"业务规则"，基础设施操作放 Service：
```csharp
// 实体:只管状态
public void MarkAsPaid() { ... }

// Service:编排基础设施
public async Task PayAsync(...)
{
    await _gateway.ChargeAsync(order.Amount);        // 基础设施
    order.MarkAsPaid();                              // 业务规则
    await _repo.UpdateAsync(order);
}
```

### 9.2 误区 2：充血必须完全 DDD

**不必**：充血模型可独立使用，不必全套 DDD（聚合根/值对象/领域事件）。

最小充血：
```csharp
public class User
{
    public string Email { get; private set; }

    public void ChangeEmail(string newEmail)
    {
        if (!IsValidEmail(newEmail)) throw new ArgumentException("...");
        Email = newEmail;
    }
}
```

### 9.3 误区 3：ORM 不支持充血

**已过时**：EF Core 2.0+、SqlSugar 都支持 private set + 工厂方法。

---

## 10. 面试话术（10 道 Q&A）

### Q1: 贫血模型和充血模型有什么区别？

> **贫血模型**：实体只有数据（属性 + getter/setter），业务逻辑在 Service 层。
> **充血模型**：实体既含数据又封装行为（方法），外部不能直接改状态，必须通过方法变更。
>
> 一句话：贫血实体是"哑数据"，充血实体是"会做事的对象"。

### Q2: 为什么 DDD 推荐充血模型？

> 三个核心原因：
> 1. **不变量保护**：private set + 行为方法，编译期保证不能绕过校验
> 2. **内聚**：业务规则集中在实体内，修改不扩散到多个 Service
> 3. **可测试性**：实体单元测试无需 Mock 基础设施，毫秒级
>
> 贫血模型的问题：任何代码都能 `order.Status = Paid` 绕过校验，业务规则散落。

### Q3: 你们项目用了充血模型吗？

> 用了。我们 [Order 聚合根](file:///c:/Users/ZJN/Desktop/jl/project/src/PaymentGateway.Domain/Orders/Order.cs) 完整实践了充血模型：
> - 属性全部 `private set`，外部不可直接改
> - 提供 `MarkAsPaying` / `MarkAsPaid` / `Close` 等行为方法
> - `EnsureCanTransit` 内部校验状态机
> - `Create` 工厂方法保证创建时不变量成立
> - `AddDomainEvent` 在状态变更同时发布领域事件

### Q4: 充血模型怎么和 ORM 配合？

> ORM 需要反射创建实例，要求 public 无参构造函数，与严格 DDD 的 private 构造冲突。
>
> 折中方案：
> 1. 暴露 public 无参构造函数（ORM 用）
> 2. 业务校验集中在工厂方法 `Create`（应用层用）
> 3. 属性 `private set`（ORM 通过反射写入，业务代码不能直接改）
>
> EF Core 2.0+ 支持 private set 和 private 构造函数，更纯净。

### Q5: 领域事件在实体还是 Service 发布？

> **实体收集，Service 发布**：
> - 实体内 `AddDomainEvent(evt)` 收集事件到内部列表
> - Service 在 `SaveChangesAsync` 事务提交后，遍历 `order.DomainEvents` 发布到消息队列
> - 发布后 `ClearDomainEvents()`
>
> 这样事件发布与状态变更在同一事务边界内，不会漏发。

### Q6: 状态机校验放在哪？

> 放在**实体内部的私有方法** `EnsureCanTransit`：
> ```csharp
> private void EnsureCanTransit(OrderStatus target)
> {
>     if (!Status.CanTransitTo(target))
>         throw new DomainException(...);
> }
> ```
> 状态转换表放在 `OrderStatus` 枚举扩展方法中，集中维护。
>
> Service 调用 `order.MarkAsPaid()` 时，实体内部自动校验，不信任 Service 会自觉。

### Q7: 贫血模型有存在的价值吗？

> 有，看场景：
> - **CRUD 后台管理**：业务简单，无状态机，贫血开发快
> - **DTO 传输对象**：纯数据载体，不需要行为
> - **查询模型（CQRS 读模型）**：只读 DTO，无业务规则
>
> 反模式是"业务复杂却用贫血"——多个 Service 改同一属性，校验逻辑复制粘贴。

### Q8: 充血模型测试有什么优势？

> 三点：
> 1. **无需 Mock**：直接 `Order.Create(...)` 创建实体，调方法断言状态
> 2. **毫秒级**：不依赖 DB / HTTP，单测秒跑
> 3. **工厂方法保证合法起点**：`Order.Create(...)` 创建的实体一定合法，不会出现"已支付但未扣款"的非法状态
>
> 贫血模型测试要 `new Order { Status = Paid }` 造非法状态，容易漏测。

### Q9: 充血模型会不会让实体膨胀？

> 不会。充血的"行为"是业务规则，不是基础设施操作。
> - 实体只做"状态变更 + 校验 + 事件发布"
> - Service 负责"加载 + 编排 + 持久化 + 发布事件"
>
> 一个 Order 实体大概 5-10 个行为方法（MarkAsPaying / MarkAsPaid / Close / Refund 等），不算膨胀。

### Q10: 值对象（Value Object）和实体（Entity）有什么区别？

> | 维度 | 实体 | 值对象 |
> |------|------|--------|
> | **标识** | 有唯一 Id | 无 Id，按属性值判等 |
> | **可变性** | 可变（状态变更） | 不可变（替换而非修改） |
> | **生命周期** | 独立跟踪 | 依附于实体 |
> | **示例** | Order、User | Money、Address |
>
> 本项目 `Money` 是值对象：
> ```csharp
> public record Money(decimal Value, string Currency = "CNY")
> {
>     public static Money Yuan(decimal v) => new(v, "CNY");
>     public Money Add(Money other) => new(Value + other.Value, Currency);
> }
> ```
> 不可变 + 按值判等，是典型的值对象。

---

## 11. 总结

| 维度 | 贫血 | 充血 |
|------|------|------|
| **何时用** | 简单 CRUD / DTO | 复杂业务 / 资金 / 状态机 |
| **实体** | 数据容器 | 数据 + 行为 |
| **Service** | 业务逻辑集中 | 编排（加载 + 调用 + 持久化） |
| **校验** | Service 内 if | 实体内 EnsureXxx |
| **测试** | 需 Mock | 实体单测秒跑 |
| **DDD 纯度** | 反模式 | 推荐 |

**核心原则**：
> **把业务规则放回它该在的地方——实体内部。**
> Service 不是业务逻辑的家，实体才是。
