using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace PaymentGateway.Infrastructure.Tracing;

/// <summary>
/// OpenTelemetry 链路追踪 DI 扩展 —— 通过 OTLP 导出到 Jaeger
/// 学习要点:
///   1. OpenTelemetry (简称 OTel) 是 CNCF 主导的可观测性标准,定义了 Trace/Span/Baggage 抽象
///      Jaeger / Tempo / Zipkin / Datadog 都兼容 OTel SDK,业务代码只依赖 OTel 抽象,与后端解耦
///   2. 现代 Jaeger (>=1.35) 原生支持 OTLP 接收协议,默认端口 4317(gRPC) / 4318(HTTP)
///      因此不再需要单独的 Jaeger.Exporter 包,统一使用 OpenTelemetry.Exporter.OpenTelemetryProtocol
///   3. AddOpenTelemetry() 注册 TracerProviderBuilder,链路构建顺序:
///        配置 Resource(服务标识) → 添加 Instrumentation(自动埋点) → 添加 Exporter(导出后端)
///   4. AspNetCore Instrumentation 自动追踪每个 HTTP 请求(生成 Span)
///      Http Instrumentation 自动追踪每个 HttpClient 调用(出站请求生成子 Span)
///      SqlClient / Redis / Kafka 等可继续叠加 Instrumentation,本工程只演示前两者
///   5. Sampler 配置:
///        AlwaysOn: 100% 采样(全量上报,生产慎用,流量大时压垮 Jaeger)
///        TraceIdRatio: 按比例采样(推荐 0.1 ~ 0.3)
///      学习工程默认 AlwaysOn,生产环境应改为 TraceIdRatio
/// </summary>
public static class JaegerTracingExtensions
{
    /// <summary>
    /// 添加 OpenTelemetry 链路追踪,通过 OTLP 导出到 Jaeger
    /// </summary>
    /// <param name="services">IServiceCollection</param>
    /// <param name="configuration">IConfiguration,读取 "Jaeger" 节</param>
    /// <returns>IServiceCollection(链式调用)</returns>
    public static IServiceCollection AddJaegerTracing(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        // ★ 读取 Jaeger 配置节 (ServiceName + OtlpEndpoint)
        // 学习要点: 配置驱动,无硬编码;读取失败时使用合理默认值
        var serviceName = configuration["Jaeger:ServiceName"] ?? "PaymentGateway";
        var otlpEndpoint = configuration["Jaeger:OtlpEndpoint"] ?? "http://localhost:4317";

        // 是否启用追踪 (开发环境可关闭以减少噪音)
        var enabled = configuration.GetValue<bool>("Jaeger:Enabled", true);

        if (!enabled)
        {
            // 学习要点: 追踪开关,本地无 Jaeger 时关闭,避免 OTLP 连接失败警告刷屏
            return services;
        }

        // ★ OpenTelemetry 注册入口
        // 学习要点: AddOpenTelemetry 是 OpenTelemetry.Extensions.Hosting 提供的扩展
        //   它返回 TracerProviderBuilder,通过 .WithTracing() 配置追踪提供者
        services.AddOpenTelemetry()
            .WithTracing(tracerBuilder =>
            {
                tracerBuilder
                    // 1) 配置 Resource —— Span 的元数据,标识来源服务
                    //    学习要点: service.name 是 W3C 推荐的必填属性,Jaeger UI 据此分组
                    .ConfigureResource(resource => resource
                        .AddService(
                            serviceName: serviceName,
                            serviceVersion: "1.0.0",
                            serviceInstanceId: Environment.MachineName))
                    // 2) 配置采样器 —— 控制上报比例,降低后端压力
                    //    学习要点: AlwaysOnSampler 全量采样,适合开发调试
                    //      生产环境改用 new TraceIdRatioBasedSampler(0.1)
                    .AddSource("PaymentGateway")           // 业务自定义 ActivitySource
                    .SetSampler(new AlwaysOnSampler())
                    // 3) 添加自动埋点 —— 不修改业务代码即可追踪常见调用
                    //    AspNetCore: 每个 HTTP 请求自动生成一个 Server Span
                    //    Http: 每个 HttpClient 调用自动生成一个 Client Span(父=当前 Server Span)
                    .AddAspNetCoreInstrumentation(opt =>
                    {
                        // 学习要点: 过滤健康检查等噪音端点,避免 Jaeger UI 充斥 /health 请求
                        opt.Filter = httpContext =>
                            !httpContext.Request.Path.StartsWithSegments("/health");
                        // 自动将 HTTP 状态码写入 Span 属性
                        opt.EnrichWithHttpRequest = (activity, request) =>
                        {
                            activity.SetTag("http.request.path", request.Path.Value);
                        };
                        opt.EnrichWithHttpResponse = (activity, response) =>
                        {
                            activity.SetTag("http.response.status_code", (int)response.StatusCode);
                        };
                    })
                    .AddHttpClientInstrumentation(opt =>
                    {
                        // 学习要点: 自动追踪所有 IHttpClientFactory 创建的 HttpClient 调用
                        //   对出站回调商户通知、调用上游渠道的场景非常关键
                        opt.EnrichWithHttpRequestMessage = (activity, request) =>
                        {
                            activity.SetTag("http.request.method", request.Method.Method);
                            // 隐藏敏感 Header (Authorization 等)
                            // 这里只记录 URI 主机部分,避免泄露 API Key 等敏感参数
                            if (request.RequestUri is not null)
                            {
                                activity.SetTag("http.host", request.RequestUri.Host);
                            }
                        };
                    })
                    // 4) 配置 OTLP Exportor —— 导出到 Jaeger (或任何 OTLP 兼容后端)
                    //    学习要点: OtlpExporter 通过 gRPC 上报 Span,默认端口 4317
                    //    生产环境应配置 Headers 携带认证 Token(Jaeger 支持 Bearer Token)
                    .AddOtlpExporter(opt =>
                    {
                        opt.Endpoint = new Uri(otlpEndpoint);
                        // 学习要点: gRPC 协议比 HTTP 性能更好,大数据量下更稳定
                        opt.Protocol = OtlpExportProtocol.Grpc;
                        // 批处理默认 5000ms / 512 条,延迟换吞吐,本地调试可缩短
                        // opt.ExportProcessorType = ExportProcessorType.Batch;
                        // opt.BatchExportProcessorOptions = new BatchExportActivityProcessorOptions
                        // {
                        //     ScheduledDelayMillis = 2000,
                        //     MaxExportBatchSize = 256
                        // };
                    });
            });

        return services;
    }
}
