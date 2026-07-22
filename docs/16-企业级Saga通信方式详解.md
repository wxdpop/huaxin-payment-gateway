# 企业级 Saga 分布式事务通信方式详解

> 一句话：编排式 Saga 用 **HTTP/gRPC 同步** + 重试；编排式 Saga 用 **消息队列异步** + 幂等。

---

## 1. 两种 Saga 通信模型对比

| 维度 | 编排式（Orchestration） | 编排式（Choreography） |
|------|----------------------|----------------------|
| **协调器** | 中央协调器（OrderSaga）指挥各服务 | 无中央协调器，各服务订阅事件自行响应 |
| **通信方式** | HTTP/gRPC 同步调用 | 消息队列（Kafka/RabbitMQ）异步事件 |
| **耦合度** | 协调器需要知道所有服务地址 | 完全解耦，服务只关心事件类型 |
| **可见性** | 流程集中在协调器，易于追踪 | 流程分散在多个服务，难追踪 |
| **事务延迟** | 毫秒级（同步链路） | 秒级（消息消费延迟） |
| **故障处理** | 协调器直接触发补偿 | 各服务监听失败事件自行补偿 |
| **典型场景** | 业务流程复杂、多分支 | 业务流程简单、事件驱动架构 |

---

## 2. 企业级典型通信架构（混合模式）

实际生产环境**很少用纯同步或纯异步**，而是混合：

```
┌─────────────────────────────────────────────────────────────┐
│                企业级 Saga 通信架构(混合模式)                │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐                                                │
│  │ Client   │                                                │
│  │ (商户系统)│                                                │
│  └────┬─────┘                                                │
│       │ HTTP + Idempotency-Key                                │
│       ▼                                                      │
│  ┌──────────────┐                                             │
│  │ API Gateway  │  ← 鉴权、限流、熔断                         │
│  └──────┬───────┘                                             │
│         │                                                     │
│         ▼                                                     │
│  ┌──────────────────────────────────┐                        │
│  │  Saga Orchestrator (协调器服务)   │                        │
│  │  - 持久化 Saga 状态到 DB         │                        │
│  │  - 调用各服务(同步 HTTP/gRPC)   │                        │
│  │  - 失败时触发补偿                │                        │
│  └────┬─────────────┬─────────────┬──┘                       │
│       │ HTTP        │ HTTP        │ HTTP                     │
│       ▼             ▼             ▼                          │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                   │
│  │Order    │   │Inventory│   │Payment  │                    │
│  │Service  │   │Service  │   │Service  │                    │
│  └────┬────┘   └────┬────┘   └────┬────┘                    │
│       │             │             │                          │
│       │ Kafka       │ Kafka       │ Kafka                    │
│       │  Event      │  Event      │  Event                   │
│       ▼             ▼             ▼                          │
│  ┌──────────────────────────────────────┐                   │
│  │      Kafka (事件总线)                 │                   │
│  │  Topics:                             │                   │
│  │  - order.created                     │                   │
│  │  - inventory.reserved                 │                   │
│  │  - payment.succeeded                  │                   │
│  │  - payment.failed                     │                   │
│  └────┬───────────────────────────────┘                   │
│       │                                                     │
│       │ 异步事件(给下游通知,不阻塞主流程)                   │
│       ▼                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │NotifySvc │  │RiskSvc   │  │Analytics │                  │
│  │(短信通知)│  │(风控告警)│  │(数据仓库) │                  │
│  └──────────┘  └──────────┘  └──────────┘                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**设计原则**：
- **同步链路**：核心事务路径（订单 → 库存 → 支付），保证低延迟
- **异步链路**：非核心通知（短信、风控、分析），削峰解耦

---

## 3. 编排式 Saga 通信详细流程（同步 HTTP）

### 3.1 同步调用链路

```
Saga Orchestrator
   │
   ├─ 1. POST /api/orders          → OrderService     (创建订单)
   ├─ 2. POST /api/inventory/reserve → InventoryService (扣库存)
   ├─ 3. POST /api/payments        → PaymentService    (发起支付)
   │
   └─ 失败时补偿:
      ├─ 3'. POST /api/payments/{id}/refund  → PaymentService
      ├─ 2'. POST /api/inventory/release    → InventoryService
      └─ 1'. POST /api/orders/{id}/cancel    → OrderService
```

### 3.2 同步调用关键代码（C# HttpClient + Polly）

```csharp
public class SagaOrchestrator
{
    private readonly HttpClient _http;
    private readonly Polly.Retry.AsyncRetryPolicy _retryPolicy;
    private readonly Polly.CircuitBreaker.AsyncCircuitBreakerPolicy _cbPolicy;

    public SagaOrchestrator(HttpClient http)
    {
        _http = http;

        // 重试策略:网络抖动自动重试 3 次,指数退避
        _retryPolicy = Policy
            .Handle<HttpRequestException>()
            .OrResult<HttpResponseMessage>(r => r.StatusCode >= System.Net.HttpStatusCode.InternalServerError)
            .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

        // 熔断器:连续 5 次失败断开 30 秒
        _cbPolicy = Policy
            .Handle<HttpRequestException>()
            .CircuitBreakerAsync(5, TimeSpan.FromSeconds(30));
    }

    public async Task<SagaResult> ExecuteAsync(SagaContext ctx)
    {
        // 用 Polly 包装:重试 + 熔断组合
        var policy = Policy.WrapAsync(_retryPolicy, _cbPolicy);

        try
        {
            // T1: 创建订单
            var orderResp = await policy.ExecuteAsync(() =>
                _http.PostAsJsonAsync("/api/orders", new { ctx.ProductCode, ctx.Quantity }));

            // T2: 扣库存
            var invResp = await policy.ExecuteAsync(() =>
                _http.PostAsJsonAsync("/api/inventory/reserve", new { ctx.OrderId, ctx.Quantity }));

            // T3: 支付
            var payResp = await policy.ExecuteAsync(() =>
                _http.PostAsJsonAsync("/api/payments", new { ctx.OrderId, ctx.Amount }));

            return SagaResult.Success(ctx);
        }
        catch (Exception ex)
        {
            // 触发补偿(同样用 policy 包装,保证重试)
            await CompensateAsync(ctx);
            return SagaResult.Failed(ex.Message, ctx);
        }
    }
}
```

### 3.3 同步通信的优缺点

**优点**：
- ✅ 延迟低（毫秒级）
- ✅ 流程清晰，易于调试
- ✅ 强一致性（失败立即知道）

**缺点**：
- ❌ 服务可用性降低（任一服务宕机整个 Saga 失败）
- ❌ 阻塞线程，吞吐受限
- ❌ 级联故障风险（需要熔断器保护）

---

## 4. 编排式 Saga 通信详细流程（异步消息）

### 4.1 事件链路

```
OrderService
   │
   │ 1. 发布 OrderCreatedEvent
   ▼
[Kafka: order.created]
   │
   │ 2. InventoryService 订阅
   ▼
InventoryService
   │
   │ 3. 发布 InventoryReservedEvent
   ▼
[Kafka: inventory.reserved]
   │
   │ 4. PaymentService 订阅
   ▼
PaymentService
   │
   │ 5. 成功 → PaymentSucceededEvent
   │    失败 → PaymentFailedEvent
   ▼
[Kafka: payment.succeeded / payment.failed]
   │
   │ 6. 失败时 OrderService 订阅 payment.failed → 取消订单
   │    InventoryService 订阅 payment.failed → 释放库存
   ▼
补偿完成
```

### 4.2 关键代码（Kafka + C#）

```csharp
// OrderService - 发布订单创建事件
public class OrderService
{
    private readonly IProducer<string, string> _producer;

    public async Task CreateOrderAsync(Order order)
    {
        // 1. 本地事务:写订单到 DB
        await _db.InsertAsync(order);

        // 2. 发布事件(用 Outbox 模式保证一致性)
        var evt = new OrderCreatedEvent(order.Id, order.ProductCode, order.Quantity);
        await _producer.ProduceAsync("order.created",
            new Message<string, string>
            {
                Key = order.Id.ToString(),
                Value = JsonSerializer.Serialize(evt)
            });
    }
}

// InventoryService - 订阅订单创建事件
public class InventoryConsumer : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _consumer.SubscribeAsync(new[] { "order.created" });

        while (!ct.IsCancellationRequested)
        {
            var msg = _consumer.Consume(ct);
            var evt = JsonSerializer.Deserialize<OrderCreatedEvent>(msg.Value);

            try
            {
                // 扣库存(本地事务)
                await _inventory.ReserveAsync(evt.ProductCode, evt.Quantity);

                // 发布库存已扣减事件
                await _producer.ProduceAsync("inventory.reserved",
                    new { evt.OrderId, evt.Quantity });
            }
            catch (Exception ex)
            {
                // 库存不足 → 发布失败事件,触发补偿
                await _producer.ProduceAsync("inventory.failed",
                    new { evt.OrderId, Reason = ex.Message });
            }
        }
    }
}
```

### 4.3 异步通信的优缺点

**优点**：
- ✅ 解耦（服务不知道彼此存在）
- ✅ 削峰（消息队列缓冲）
- ✅ 高可用（消费者宕机不阻塞生产者）

**缺点**：
- ❌ 延迟高（秒级）
- ❌ 调试困难（需要分布式追踪）
- ❌ 消息必须幂等（可能重复消费）

---

## 5. 企业级 Saga 状态持久化

### 5.1 为什么需要持久化 Saga 状态

```
场景: Saga 执行到 T2 后,T3 调用 PaymentService 时协调器宕机

  T1 ✓ → T2 ✓ → [协调器崩溃] → ???

不持久化: T1 + T2 的副作用(订单已建,库存已扣)成为孤儿
持久化:  重启协调器从 DB 读取 Saga 状态,继续执行 T3 或补偿
```

### 5.2 Saga 状态表设计

```sql
CREATE TABLE saga_instances (
    saga_id          VARCHAR(64) PRIMARY KEY,
    saga_type        VARCHAR(64) NOT NULL,      -- 'order_saga'
    status           VARCHAR(20) NOT NULL,      -- 'running'/'completed'/'compensating'/'failed'
    current_step     INT NOT NULL,                -- 当前执行到第几步
    context_data     JSONB NOT NULL,              -- SagaContext 序列化
    created_at       TIMESTAMP NOT NULL,
    updated_at       TIMESTAMP NOT NULL,
    completed_at     TIMESTAMP,
    failure_reason   TEXT
);

CREATE TABLE saga_steps (
    id               BIGSERIAL PRIMARY KEY,
    saga_id          VARCHAR(64) NOT NULL,
    step_index       INT NOT NULL,
    step_name        VARCHAR(50) NOT NULL,
    status           VARCHAR(20) NOT NULL,       -- 'pending'/'success'/'failed'/'compensated'
    request_data     JSONB,
    response_data    JSONB,
    started_at       TIMESTAMP,
    completed_at     TIMESTAMP,
    FOREIGN KEY (saga_id) REFERENCES saga_instances(saga_id)
);
```

### 5.3 状态机

```
                +-------------+
                |  Running    | ← 新建 Saga
                +------+------+
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
+-------+----+ +-------+------+ +------+--------+
| Completed | | Compensating | |    Failed     |
| (全部成功) | | (执行补偿中) | | (补偿失败,需人工)|
+------------+ +-------+------+ +---------------+
                       │
                       ▼
                +------+------+
                | Compensated | ← 补偿完成
                +-------------+
```

---

## 6. 幂等性保障（企业级关键）

### 6.1 为什么必须幂等

```
场景: 协调器调用 InventoryService.ReserveAsync 后超时
  - 实际上库存已扣成功
  - 协调器不知道,触发重试 → 扣两次?

解决: 业务 ID 幂等(reservation_id)
  - 第一次调用:扣库存 + 记录 reservation_id
  - 重试时:发现 reservation_id 已存在 → 直接返回成功
```

### 6.2 幂等实现（业务 ID 表）

```sql
CREATE TABLE idempotent_requests (
    request_id    VARCHAR(64) PRIMARY KEY,
    service_name  VARCHAR(50) NOT NULL,
    operation     VARCHAR(50) NOT NULL,
    response_data JSONB,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 调用前检查
SELECT * FROM idempotent_requests WHERE request_id = ?;
```

### 6.3 幂等代码模式

```csharp
public class InventoryService
{
    public async Task ReserveAsync(string reservationId, string productCode, int qty)
    {
        // 1. 幂等检查
        var existing = await _idempotentRepo.GetAsync(reservationId);
        if (existing is not null)
        {
            // 已处理过,直接返回(不重复扣减)
            return;
        }

        // 2. 执行业务(本地事务,保证原子性)
        using var tx = await _db.BeginTransactionAsync();
        try
        {
            await _inventory.ReserveAsync(productCode, qty);
            await _idempotentRepo.InsertAsync(reservationId, "inventory.reserve", qty);
            await tx.CommitAsync();
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
    }
}
```

---

## 7. 消息队列的可靠性保证（Outbox 模式）

### 7.1 Outbox 模式解决的问题

```
场景: 业务操作 + 消息发送 必须原子化

错误做法:
  await db.InsertAsync(order);        // 1. 写订单
  await kafka.SendAsync(evt);         // 2. 发事件
  // 如果 1 成功 2 失败 → 订单存在但事件丢失

Outbox 模式:
  await db.InsertAsync(order);
  await db.InsertAsync(outboxMsg);   // 写入 outbox 表(同一事务)
  // 后台轮询 outbox 表,发送到 Kafka,发送后删除
```

### 7.2 Outbox 表设计

```sql
CREATE TABLE outbox_messages (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type   VARCHAR(50) NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP
);

-- 业务事务内同时写
BEGIN;
INSERT INTO orders (...) VALUES (...);
INSERT INTO outbox_messages (aggregate_id, event_type, payload)
    VALUES ('order-123', 'OrderCreated', '{"orderId":123,...}');
COMMIT;

-- 后台轮询发送
SELECT * FROM outbox_messages WHERE processed_at IS NULL ORDER BY id LIMIT 100;
-- 发送成功后
UPDATE outbox_messages SET processed_at = NOW() WHERE id = ?;
```

---

## 8. 与本项目 SagaDemo 的对比

| 维度 | 本项目 SagaDemo | 企业级生产 |
|------|---------------|-----------|
| **服务通信** | 进程内方法调用 | HTTP/gRPC + Kafka |
| **状态持久化** | 内存（重启丢失） | PostgreSQL 表 |
| **幂等性** | HashSet 简单去重 | DB 唯一约束 + 业务 ID |
| **重试/熔断** | 无 | Polly + 熔断器 |
| **消息可靠** | 无 | Outbox 模式 |
| **分布式追踪** | 无 | OpenTelemetry + Jaeger |
| **监控告警** | 无 | Prometheus + AlertManager |

**本项目 SagaDemo 的价值**：
- 学习 Saga 核心思想（补偿事务、反向顺序、幂等）
- 不引入复杂基础设施，专注业务逻辑理解
- 后续可逐步演进：内存 → Kafka、单机 → 分布式

---

## 9. 选型决策树

```
业务流程是否复杂(>3 个分支)?
├─ 是 → 编排式(Orchestration) + HTTP 同步
│        + 持久化 Saga 状态 + Polly 重试熔断
└─ 否 → 业务流程是否强调实时性?
        ├─ 是 → 编排式 + HTTP 同步
        └─ 否 → 编排式(Choreography) + Kafka 异步
                 + Outbox 模式 + 事件溯源

是否需要跨语言?
├─ 是 → gRPC(HTTP/2 + Protobuf)
└─ 否 → HTTP REST(JSON)

是否需要严格顺序?
├─ 是 → Kafka 单分区(全局有序)
└─ 否 → Kafka 多分区(并行消费)
```

---

## 10. 面试话术（8 道 Q&A）

### Q1: Saga 在企业级系统中怎么通信？

> 我们用 **混合模式**：核心事务链路用 HTTP/gRPC 同步调用，保证毫秒级延迟和强一致语义；非核心通知（短信、风控、分析）用 Kafka 异步事件，削峰解耦。
>
> 同步链路配 Polly 重试+熔断，避免级联故障；异步链路用 Outbox 模式保证消息不丢。

### Q2: 编排式和编排式 Saga 怎么选？

> 看流程复杂度：
> - **编排式（Orchestration）**：流程复杂、有分支、需要中央管控 → 协调器同步调用各服务
> - **编排式（Choreography）**：流程简单、事件驱动、服务解耦 → Kafka 事件链
>
> 我们订单 Saga 用编排式，因为订单→库存→支付有明确顺序和补偿逻辑，集中管理易调试。

### Q3: Saga 状态为什么要持久化？

> 协调器宕机后，已执行的本地事务成为孤儿。持久化 Saga 状态到 DB，重启时能恢复：
> - 读取 saga_instances 表，找到 status=running 的 Saga
> - 检查 saga_steps 表，找到最后成功的步骤
> - 从下一步继续执行，或触发补偿

### Q4: Saga 调用超时了怎么办？

> 三层处理：
> 1. **重试**：Polly 指数退避重试 3 次
> 2. **幂等**：每次调用带 `Idempotency-Key`，重试不会重复扣减
> 3. **超时 + 补偿**：超过最大重试次数 → 标记 Saga 失败 → 触发补偿事务

### Q5: 异步 Saga 怎么保证消息不丢？

> 用 **Outbox 模式**：
> 1. 业务事务内同时写 outbox_messages 表（原子性）
> 2. 后台轮询 outbox 表，发送到 Kafka，成功后更新 processed_at
> 3. 消费端幂等处理，重复消息不产生副作用

### Q6: 为什么 Saga 不用 2PC（两阶段提交）？

> 2PC 的问题：
> 1. **同步阻塞**：所有参与者锁资源直到协调器发提交，长事务持锁影响吞吐
> 2. **单点故障**：协调器宕机所有参与者阻塞
> 3. **性能差**：跨网络多次 RTT，不适合互联网高并发
>
> Saga 拆成长事务序列 + 补偿，每步快速提交，最终一致性，吞吐高。

### Q7: 补偿事务失败了怎么办？

> 补偿失败不能放弃，必须重试 + 告警：
> 1. **重试队列**：失败的补偿进死信队列，定时重试
> 2. **人工干预**：重试 N 次仍失败 → 发告警（短信/电话）→ 运维介入
> 3. **审计对账**：T+1 跑对账脚本，找出不一致数据修复

### Q8: Saga 和 TCC 怎么选？

> - **TCC**（Try-Confirm-Cancel）：强一致、资源预占、性能略低，适合资金核心链路（如转账）
> - **Saga**：最终一致、无预占、吞吐高，适合订单/库存等业务链路
>
> 我们支付系统两种都用：核心转账用 TCC 保证强一致，订单流程用 Saga 提升吞吐。

---

## 11. 总结

| 场景 | 通信方式 | 协议 | 持久化 |
|------|---------|------|--------|
| **核心事务链路** | HTTP/gRPC 同步 | REST/Protobuf | Saga 状态表 |
| **非核心通知** | Kafka 异步 | JSON/Protobuf | Outbox 表 |
| **跨语言调用** | gRPC | Protobuf | - |
| **跨机房** | Kafka 多机房复制 | JSON | Outbox + 死信队列 |

**企业级核心要素**：
1. ✅ Saga 状态持久化（重启可恢复）
2. ✅ 同步链路：Polly 重试 + 熔断
3. ✅ 异步链路：Outbox 模式保证不丢
4. ✅ 全链路：幂等性（业务 ID）
5. ✅ 可观测性：OpenTelemetry + Prometheus + Jaeger
6. ✅ 人工兜底：死信队列 + 告警 + 对账
