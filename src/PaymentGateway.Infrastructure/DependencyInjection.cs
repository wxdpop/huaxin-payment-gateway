using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;  // ★ Configure<T> 扩展方法所在
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Domain.Accounts;
using PaymentGateway.Domain.Orders;
using PaymentGateway.Domain.Payments;
using PaymentGateway.Domain.Refunds;
using PaymentGateway.Infrastructure.Cache;
using PaymentGateway.Infrastructure.Channels;
using PaymentGateway.Infrastructure.Common;
using PaymentGateway.Infrastructure.DistributedLock;
using PaymentGateway.Infrastructure.DistributedLock.Redis;
using PaymentGateway.Infrastructure.DistributedLock.ZooKeeper;
using PaymentGateway.Infrastructure.EventBus;
using PaymentGateway.Infrastructure.Idempotent;
using PaymentGateway.Infrastructure.Persistence;
using PaymentGateway.Infrastructure.Persistence.Repositories;
using PaymentGateway.Infrastructure.Tracing;

namespace PaymentGateway.Infrastructure;

/// <summary>
/// Infrastructure 层 DI 注册扩展
/// 学习要点:
///   1. AddInfrastructure 注册: SqlSugar + 仓储 + UoW + 事件分发器 + 分布式锁 + 缓存
///   2. SqlSugar 用 Singleton(SqlSugarScope 内部 AsyncLocal 隔离)
///   3. 仓储使用 Scoped 与请求生命周期一致(虽然 ISqlSugarClient 是单例)
///   4. 分布式锁和缓存服务用 Singleton (长生命周期,持有 ConnectionMultiplexer)
/// </summary>
public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // 注册 SqlSugar (ISqlSugarClient 单例,内部 AsyncLocal 隔离请求)
        services.AddSqlSugar(configuration);

        // 注册仓储(Scoped 与请求生命周期一致)
        services.AddScoped<IOrderRepository, OrderRepository>();
        services.AddScoped<IPaymentRepository, PaymentRepository>();
        services.AddScoped<IAccountRepository, AccountRepository>();
        services.AddScoped<IRefundRepository, RefundRepository>();

        // 注册 UoW 与事件分发器
        services.AddScoped<IUnitOfWork, UnitOfWork>();
        services.AddScoped<IDomainEventDispatcher, DomainEventDispatcher>();

        // ============================================================================
        // M2: 分布式锁 + 缓存服务注册
        // ============================================================================
        // 学习要点: 分布式锁 Provider 用 Singleton (持有 Redis/ZK 连接,长生命周期)
        //   - ConnectionMultiplexer 是线程安全的,可被多请求共享
        //   - 避免每个请求创建连接(昂贵)

        // 1. 绑定 LockOptions 配置节
        services.Configure<LockOptions>(configuration.GetSection("DistributedLock"));

        // 2. 注册各类型 Provider (Singleton 持有连接)
        //    学习要点: 通过工厂方法注册,确保 RedLockProvider 与 ZooKeeperLockProvider 单例
        services.AddSingleton<RedLockProvider>();
        services.AddSingleton<ZooKeeperLockProvider>();

        // 3. 注册 DualLockProvider (依赖前两个 Singleton)
        services.AddSingleton<DualLockProvider>();

        // 4. ★ 根据 DistributedLockType 配置选择默认 IDistributedLock 实现
        //    学习要点: 业务依赖 IDistributedLock 抽象,配置决定具体实现
        //    - 支付回调场景配置 Redlock (性能优先)
        //    - 资金账户变更场景配置 Dual (强一致优先)
        //    生产中可同时注册多个 Named Lock,业务按需选择
        var lockTypeStr = configuration["DistributedLock:DefaultType"] ?? "Dual";
        if (Enum.TryParse<DistributedLockType>(lockTypeStr, ignoreCase: true, out var lockType))
        {
            services.AddSingleton<IDistributedLock>(sp =>
            {
                // ★ 学习要点: 工厂模式根据配置返回不同实现
                //   这是"策略模式 + DI"的常见做法,运行时根据配置选择具体实现
                return lockType switch
                {
                    DistributedLockType.Redlock => sp.GetRequiredService<RedLockProvider>(),
                    DistributedLockType.ZooKeeper => sp.GetRequiredService<ZooKeeperLockProvider>(),
                    DistributedLockType.Dual => sp.GetRequiredService<DualLockProvider>(),
                    _ => sp.GetRequiredService<DualLockProvider>()
                };
            });
        }
        else
        {
            services.AddSingleton<IDistributedLock, DualLockProvider>();
        }

        // ============================================================================
        // M2: Redis-Cluster 缓存服务注册
        // ============================================================================
        services.Configure<CacheOptions>(configuration.GetSection("Redis"));
        services.AddSingleton<ICacheService, RedisCacheService>();

        // ============================================================================
        // M3: Kafka 事件总线注册
        // ============================================================================
        services.Configure<EventBusOptions>(configuration.GetSection("EventBus"));

        // ★ 学习要点: 根据 UseInMemory 配置选择 IEventBus 实现
        //   - UseInMemory=true: 用 InMemoryEventBus (无 Kafka 环境的本地开发)
        //   - UseInMemory=false: 用 KafkaEventBus (生产场景)
        var useInMemory = configuration.GetValue<bool>("EventBus:UseInMemory");
        if (useInMemory)
        {
            services.AddSingleton<IEventBus, InMemoryEventBus>();
        }
        else
        {
            services.AddSingleton<IEventBus, KafkaEventBus>();
        }

        // ============================================================================
        // M4: OpenTelemetry 链路追踪 (OTLP → Jaeger)
        // ============================================================================
        // 学习要点: AddJaegerTracing 内部会读取 "Jaeger" 配置节
        //   - 启用后 ASP.NET Core 自动埋点每个 HTTP 请求,HttpClient 调用自动埋点
        //   - 业务代码通过 TraceContext.StartSpan() 添加自定义 Span
        services.AddJaegerTracing(configuration);

        // ============================================================================
        // M4: ZeroMQ 通道通信 (REQ/REP + PUB/SUB)
        // ============================================================================
        services.Configure<ZeroMqOptions>(configuration.GetSection("ZeroMq"));

        // 学习要点: 仅在 ZeroMq:Enabled=true 时注册 socket 单例
        //   - 单例持有 socket,避免每次创建(ZeroMQ 连接建立有成本)
        //   - NetMQ 在应用退出时需要调用 NetMQConfig.Cleanup() 释放资源
        var zeroMqEnabled = configuration.GetValue<bool>("ZeroMq:Enabled", true);
        if (zeroMqEnabled)
        {
            services.AddSingleton<ZeroMqReqRepClient>();
            services.AddSingleton<ZeroMqPubSubClient>();
            // ★ 注册 IHostedService,应用退出时自动调用 NetMQConfig.Cleanup()
            services.AddHostedService<ZeroMqCleanupHostedService>();
        }

        // ============================================================================
        // M4: HTTP 幂等服务 (基于 ICacheService 复用 Redis)
        // ============================================================================
        // 学习要点: IdempotentService 是 Scoped 还是 Singleton?
        //   - ICacheService 是 Singleton,可被 Singleton 安全注入
        //   - ILogger<T> 也是 Singleton
        //   - IdempotentService 自身无状态 → Singleton 即可
        //   但为了配合 HTTP 请求上下文(后续可能扩展 ICurrentUser),
        //   这里用 Scoped 与请求生命周期一致
        services.Configure<IdempotentOptions>(configuration.GetSection("Idempotent"));
        services.AddScoped<IdempotentService>();

        return services;
    }
}
