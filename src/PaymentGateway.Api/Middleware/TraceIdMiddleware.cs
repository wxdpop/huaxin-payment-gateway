namespace PaymentGateway.Api.Middleware;

/// <summary>
/// TraceId 注入中间件 —— 把请求 TraceId 写入响应头,便于前端关联日志
/// 学习要点:
///   1. ASP.NET Core 自动为每个请求生成 TraceIdentifier(默认 W3C TraceContext)
///   2. 此中间件把 TraceId 写入响应头 X-Trace-Id,前端可记录并附在后续请求中
///   3. 后续 M4 接入 OpenTelemetry,TraceId 会与 Jaeger Span 链路关联
/// </summary>
public class TraceIdMiddleware
{
    private readonly RequestDelegate _next;

    public TraceIdMiddleware(RequestDelegate next) => _next = next;

    public async Task InvokeAsync(HttpContext context)
    {
        // 把 TraceId 写入响应头,前端可记录用于关联日志/链路
        context.Response.OnStarting(() =>
        {
            context.Response.Headers["X-Trace-Id"] = context.TraceIdentifier;
            return Task.CompletedTask;
        });

        await _next(context);
    }
}
