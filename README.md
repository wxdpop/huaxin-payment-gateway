# 华鑫融汇聚合支付网关

面向 .NET Web 中高级工程师的聚合支付网关学习工程，覆盖 DDD 分层、分布式锁、事件驱动、链路追踪、可观测性等核心技术点。

## 技术栈

| 分类 | 技术 |
|------|------|
| 运行时 | .NET 8 / ASP.NET Core 8 |
| ORM | SqlSugar |
| 数据库 | MySQL 8.0 |
| 分布式锁 | Redis Redlock (6 独立实例) + ZooKeeper 临时顺序节点 + 双重锁 |
| 事件总线 | Kafka (Confluent 7.5) + 内存模式 |
| 缓存 | Redis (StackExchange.Redis) |
| 链路追踪 | OpenTelemetry + Jaeger |
| 指标监控 | prometheus-net.AspNetCore + Prometheus + Grafana |
| 日志 | Serilog (CompactJsonFormatter) |
| 通道通信 | ZeroMQ (PubSub / ReqRep) |
| 幂等 | Redis + DB 唯一约束 + HTTP Idempotency-Key |
| 容器化 | Docker + docker-compose |

## 代码结构

```
PaymentGateway.sln / .slnx
├── src/
│   ├── PaymentGateway.Api              # ASP.NET Core 宿主层
│   │   ├── Endpoints/                  # Minimal API 路由 (订单/支付/账户)
│   │   ├── Consumers/                  # Kafka 事件消费者 (入账/商户通知)
│   │   └── Middleware/                 # 异常/幂等/TraceId 中间件
│   │
│   ├── PaymentGateway.Application      # 应用层 (用例编排, 无基础设施依赖)
│   │   ├── Abstractions/               # 仓储/锁/事件总线/工作单元 接口
│   │   ├── Orders/                     # 创建订单 Command + 查询
│   │   ├── Payments/                   # 发起支付/回调处理/退款
│   │   └── EventBus/                   # 事件定义 + Topic 常量
│   │
│   ├── PaymentGateway.Domain           # 领域层 (纯 C#, 零外部依赖)
│   │   ├── Accounts/                   # 资金账户聚合根 + 流水
│   │   ├── Orders/                     # 订单聚合根 + 状态机 + 领域事件
│   │   ├── Payments/                   # 支付记录
│   │   └── Shared/                     # Money 值对象/实体/聚合根基类
│   │
│   ├── PaymentGateway.Infrastructure   # 基础设施层 (接口实现)
│   │   ├── DistributedLock/            # Redis Redlock / ZK / DualLock
│   │   ├── EventBus/                   # Kafka 生产消费 + InMemory 实现
│   │   ├── Persistence/                # SqlSugar 仓储 + 乐观锁
│   │   ├── Cache/                      # Redis 缓存 + 防穿透雪崩
│   │   ├── Tracing/                    # OpenTelemetry + Jaeger Span 封装
│   │   ├── Metrics/                    # Prometheus 自定义业务指标
│   │   ├── Channels/                   # ZeroMQ 通道
│   │   └── Idempotent/                 # 幂等服务
│   │
│   └── PaymentGateway.Shared           # 共享内核
│       ├── Exceptions/                 # BusinessException / ConcurrencyException
│       └── Results/                    # 统一返回 Result<T>
│
├── tests/
│   └── PaymentGateway.Domain.Tests     # 领域层单元测试 (xUnit)
│
└── deploy/
    ├── Dockerfile                      # 多阶段构建
    ├── docker-compose.yml              # 中间件 + API 一键编排
    └── infra/                          # Prometheus / Grafana / MySQL 配置
```

## 快速开始

```bash
# 1. 启动全部中间件 + API 容器
docker-compose -f deploy/docker-compose.yml up -d --build

# 2. 验证
# API:        http://localhost:5000/health
# Prometheus: http://localhost:9090/targets
# Grafana:    http://localhost:3000  (admin/admin)
# Jaeger:     http://localhost:16686

# 3. 触发完整支付链路
curl -X POST http://localhost:5000/api/v1/orders/ \
  -H "Content-Type: application/json" \
  -d '{"merchantId":1,"outTradeNo":"TEST001","amount":1.00,"subject":"测试","channelCode":"wechat"}'
```

## 依赖方向

```
Api → Application → Domain
        ↑              ↑
Infrastructure ────────┘
     ↓
  Shared
```

Application 层定义接口抽象，Infrastructure 层实现，Api 层编排。Domain 不依赖任何外部包。
