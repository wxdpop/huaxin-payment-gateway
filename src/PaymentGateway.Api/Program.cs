using PaymentGateway.Api.Consumers;
using PaymentGateway.Api.Endpoints;
using PaymentGateway.Api.Middleware;
using PaymentGateway.Application;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Application.EventBus;
using PaymentGateway.Infrastructure;
using PaymentGateway.Infrastructure.EventBus;
using Prometheus;
using Serilog;
using Serilog.Formatting.Compact;

// ============================================================================
// ★ M6-3: Serilog 结构化日志初始化 (在 Host 构建前配置,确保启动阶段日志也走 Serilog)
// 学习要点:
//   1. 传统 ILogger 输出纯文本,难以被 ELK/Loki 解析;Serilog 用 CompactJsonFormatter
//      输出 JSON,每条日志一个 JSON 对象(含 @t 时间戳/@mt 模板/@l 级别/自定义字段)
//   2. Enrich.FromLogContext: 注入 LogContext 里的属性(如 TraceId/UserId)
//   3. MinimumLevel.Override("Microsoft", Warning): 抑制框架噪音日志(路由匹配/静态文件)
//   4. 此处用代码配置;生产可改用 .ReadFrom.Configuration(builder.Configuration)
//      从 appsettings.json 的 "Serilog" 节读取(运行时可改无需重编译)
// ============================================================================
Log.Logger = new LoggerConfiguration()
    .Enrich.FromLogContext()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Warning)
    .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
    .WriteTo.Console(new CompactJsonFormatter())
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);

// ★ M6-3: 用 Serilog 替换内置日志系统
//   UseSerilog() 让 ASP.NET Core 的 ILogger 全部走 Serilog 管道
builder.Host.UseSerilog();

// ============================================================================
// 服务注册阶段(IServiceCollection)
// 学习要点: 分层注册,每层只暴露必要的接口给上层使用
// ============================================================================

// Application 层: 应用服务(IXxxService 接口) + 查询服务
// 学习要点: 已移除 MediatR,采用"应用服务接口 + 实现"模式组织 CQRS
builder.Services.AddApplication();

// Infrastructure 层: SqlSugar + 仓储 + UoW + 事件分发器 + 分布式锁 + 缓存 + 事件总线
builder.Services.AddInfrastructure(builder.Configuration);

// ★ M3: Kafka 消费者宿主注册 (BackgroundService)
//   学习要点: AddHostedService 注册 IHostedService,随应用启动自动启动消费循环
//   - UseInMemory=false (Kafka 模式): 注册为 HostedService,自动启动 Kafka 消费循环
//   - UseInMemory=true  (内存模式): 注册为普通 Singleton(不启动 Kafka 循环),
//     改由 InMemoryEventBus.Subscribe 在进程内触发同一份处理逻辑
//     ★ 这样保证两种事件总线实现下的业务逻辑完全一致(同一份代码,两种触发方式)
var useInMemoryEventBus = builder.Configuration.GetValue<bool>("EventBus:UseInMemory");
if (!useInMemoryEventBus)
{
    builder.Services.AddHostedService<CreditAccountConsumer>();
    builder.Services.AddHostedService<MerchantNotifyConsumer>();
}
else
{
    // 内存模式: 注册为普通 Singleton,供 InMemoryEventBus 订阅时调用
    //   注意: 不用 AddHostedService,避免启动 Kafka 消费循环(会连接 Kafka 失败)
    builder.Services.AddSingleton<CreditAccountConsumer>();
    builder.Services.AddSingleton<MerchantNotifyConsumer>();
}

// API 层: 健康检查(供 Docker/K8s 探针使用)
// 学习要点: /health 端点返回 200 表示服务存活
//   M4 阶段会通过 AspNetCore.HealthChecks.Npgsql 包加入 DB 健康检查
builder.Services.AddHealthChecks();

// CORS: 学习工程全开放,生产环境应限制来源域名
builder.Services.AddCors(opt =>
    opt.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));

var app = builder.Build();

// ============================================================================
// ★ 内存事件总线订阅注册 (UseInMemory=true 时)
//   学习要点: Kafka 模式下消费者由 HostedService 自动启动消费循环;
//     内存模式下无消费循环,需在应用启动时把 Consumer 的处理方法注册为 InMemoryEventBus 订阅者,
//     这样 HandleCallbackService.PublishAsync(PaymentSucceededEvent) 时会同步触发入账逻辑。
//     保证两种模式下"回调→入账→通知商户"的链路一致。
// ============================================================================
if (useInMemoryEventBus && app.Services.GetRequiredService<IEventBus>() is InMemoryEventBus inMemoryBus)
{
    var creditConsumer = app.Services.GetRequiredService<CreditAccountConsumer>();
    var notifyConsumer = app.Services.GetRequiredService<MerchantNotifyConsumer>();

    // 订阅支付成功事件 → 入账
    inMemoryBus.Subscribe<PaymentSucceededEvent>(
        PaymentEventTopics.PaymentSucceeded,
        e => creditConsumer.HandlePaymentSucceededAsync(e, default));

    // 订阅账户入账事件 → 通知商户
    inMemoryBus.Subscribe<AccountCreditedEvent>(
        PaymentEventTopics.AccountCredited,
        e => notifyConsumer.HandleAccountCreditedAsync(e, default));
}

// ============================================================================
// 中间件管道配置(IApplicationBuilder)
// 学习要点: 中间件顺序至关重要,异常处理必须最先注册以捕获后续所有异常
// ============================================================================

// 全局异常处理(第一个注册,捕获后续所有中间件抛出的异常)
app.UseMiddleware<ExceptionMiddleware>();

// TraceId 注入(响应头携带 X-Trace-Id)
app.UseMiddleware<TraceIdMiddleware>();

// ★ M4: HTTP 幂等中间件 (POST/PUT/PATCH 拦截,基于 Idempotency-Key 头)
//   顺序说明: 异常处理 → TraceId → 幂等检查 → CORS → Endpoints
//   幂等检查放在业务端点之前,可拦截重复请求避免业务执行
app.UseMiddleware<IdempotencyMiddleware>();

app.UseCors();

// ★ M6-3: Prometheus HTTP 指标中间件
//   学习要点: UseHttpMetrics 自动采集 http_requests_received_total / http_request_duration_seconds
//   放在 CORS 之后、端点之前,确保能拦截所有业务请求
app.UseHttpMetrics();

// 健康检查端点
app.MapHealthChecks("/health");
app.MapHealthChecks("/health/ready");

// ★ M6-3: Prometheus 指标暴露端点 (/metrics)
//   学习要点: Prometheus 采用 Pull 模型,定时抓取此端点获取指标数据
//   暴露的指标包括: http_* (自动) + paymentgateway_* (PaymentMetrics 自定义)
app.MapMetrics("/metrics");

// 订单端点
app.MapOrderEndpoints();

// ★ M5: 支付端点(发起支付/渠道回调/退款)
app.MapPaymentEndpoints();

// ★ M5: 账户端点(查询余额)
app.MapAccountEndpoints();

// 根路径欢迎信息(便于浏览器快速验证服务运行)
app.MapGet("/", () => new
{
    Service = "华鑫融汇聚合支付网关 - 学习工程",
    Version = "v1",
    Swagger = "/swagger",
    Health = "/health",
    Docs = "/docs"
});

// ★ M4: NetMQ 资源清理已通过 ZeroMqCleanupHostedService (IHostedService) 自动注册
//   Infrastructure 层在 AddInfrastructure() 中调用 services.AddHostedService<ZeroMqCleanupHostedService>()
//   应用退出时 StopAsync 自动调用 NetMQConfig.Cleanup() 释放后台 Poller 线程

// ★ M6-3: 用 try-finally 确保 Serilog 缓冲区刷新
//   学习要点: Log.CloseAndFlush() 保证应用退出前所有异步日志已写入 sink
//   否则程序异常终止可能丢失最后几条日志
try
{
    app.Run();
}
finally
{
    Log.CloseAndFlush();
}
