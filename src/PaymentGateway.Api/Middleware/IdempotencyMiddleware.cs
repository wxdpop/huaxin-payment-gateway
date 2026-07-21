using System.Text;
using Microsoft.Extensions.Options;
using PaymentGateway.Infrastructure.Idempotent;
using PaymentGateway.Infrastructure.Tracing;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Middleware;

/// <summary>
/// HTTP 幂等中间件 —— 基于 Idempotency-Key 请求头实现 POST/PUT 请求幂等
/// 学习要点:
///   1. 中间件顺序:
///        ExceptionMiddleware → TraceIdMiddleware → IdempotencyMiddleware → CORS → Endpoints
///        放在 TraceId 之后便于追踪,在 Endpoints 之前便于拦截请求
///   2. 仅对 POST/PUT/PATCH 生效(GET/DELETE 天然幂等,无需此中间件)
///   3. 三种响应场景:
///        - 首次请求: 占用 processing 标记 → 调用下游 → 写入响应缓存 → 返回
///        - 重复请求(已完成): 直接返回缓存的首次响应
///        - 并发请求(processing 中): 返回 409 Conflict
///   4. 与支付业务双重幂等的配合:
///        - HTTP 层幂等: 防止客户端重试导致的重复请求(基于 Idempotency-Key)
///        - 业务层幂等: 防止同一业务单号重复扣款(基于 biz_no 唯一约束)
///        两者互补,HTTP 层是"前置过滤",业务层是"最终防线"
/// </summary>
public class IdempotencyMiddleware
{
    private readonly RequestDelegate _next;
    private readonly IdempotentOptions _options;
    private readonly ILogger<IdempotencyMiddleware> _logger;

    // 需要幂等保护的 HTTP 方法
    private static readonly HashSet<string> ProtectedMethods = new(StringComparer.OrdinalIgnoreCase)
    {
        "POST", "PUT", "PATCH"
    };

    public IdempotencyMiddleware(
        RequestDelegate next,
        IOptions<IdempotentOptions> options,
        ILogger<IdempotencyMiddleware> logger)
    {
        _next = next;
        _options = options.Value;
        _logger = logger;
    }

    public async Task InvokeAsync(
        HttpContext context,
        IdempotentService idempotentService)
    {
        // 非保护方法直接放行
        if (!ProtectedMethods.Contains(context.Request.Method))
        {
            await _next(context);
            return;
        }

        // 读取 Idempotency-Key 头部
        if (!context.Request.Headers.TryGetValue(_options.HeaderName, out var keyValues) ||
            string.IsNullOrWhiteSpace(keyValues.ToString()))
        {
            // 未携带 Idempotency-Key 的非幂等请求: 允许通过但记录警告
            // 学习要点: 生产环境可改为强制要求,缺失则返回 400
            _logger.LogDebug("POST/PUT 请求未携带 {Header} 头部,跳过幂等保护: {Path}",
                _options.HeaderName, context.Request.Path);
            await _next(context);
            return;
        }

        var idempotencyKey = keyValues.ToString()!;

        // ★ 业务 Span 埋点
        using var span = TraceContext.StartSpan(
            "idempotent.check",
            ("idempotency.key", idempotencyKey),
            ("http.method", context.Request.Method));

        // 阶段 1: 尝试抢占幂等标记
        var (isFirst, existing) = await idempotentService.TryAcquireAsync(idempotencyKey);

        if (!isFirst && existing is not null)
        {
            span?.SetTag("idempotent.hit", true);

            if (existing.Status == "completed")
            {
                // 场景 B: 重复请求(已完成) —— 直接返回缓存响应
                _logger.LogInformation(
                    "幂等命中(已完成): key={Key} 返回缓存响应 statusCode={StatusCode}",
                    idempotencyKey, existing.StatusCode);

                await WriteCachedResponseAsync(context, existing);
                return;
            }

            if (existing.Status == "processing")
            {
                // 场景 C: 并发请求(处理中) —— 返回 409 防止重复执行
                _logger.LogWarning(
                    "幂等命中(处理中): key={Key} 返回 409 防止并发执行", idempotencyKey);
                span?.SetTag("idempotent.conflict", true);

                context.Response.StatusCode = StatusCodes.Status409Conflict;
                context.Response.ContentType = "application/json";
                var conflict = new Result
                {
                    Success = false,
                    Code = "IDEMPOTENT_CONFLICT",
                    Message = "请求正在处理中,请稍后重试",
                    TraceId = context.TraceIdentifier
                };
                await context.Response.WriteAsJsonAsync(conflict, context.RequestAborted);
                return;
            }
        }

        // 场景 A: 首次请求 —— 拦截响应体并写入缓存
        // 学习要点: 用 MemoryStream 拦截原始响应,业务无感知
        var originalBodyStream = context.Response.Body;
        using var responseBodyMemoryStream = new MemoryStream();
        context.Response.Body = responseBodyMemoryStream;

        try
        {
            await _next(context);

            // 业务执行完成,捕获响应体
            responseBodyMemoryStream.Seek(0, SeekOrigin.Begin);
            var responseBody = await new StreamReader(responseBodyMemoryStream).ReadToEndAsync();

            // 写回原始响应流
            responseBodyMemoryStream.Seek(0, SeekOrigin.Begin);
            await responseBodyMemoryStream.CopyToAsync(originalBodyStream, context.RequestAborted);

            // 阶段 2: 写入完成响应缓存(供后续重复请求使用)
            await idempotentService.CompleteAsync(
                idempotencyKey,
                context.Response.StatusCode,
                responseBody,
                context.Response.ContentType,
                context.RequestAborted);

            _logger.LogInformation(
                "幂等记录完成: key={Key} statusCode={StatusCode}",
                idempotencyKey, context.Response.StatusCode);
        }
        catch (Exception ex)
        {
            // 业务异常: 释放 processing 标记,允许客户端重试
            // 学习要点: 异常时不写入缓存,让客户端可以重试
            TraceContext.RecordError(ex);
            await idempotentService.ReleaseAsync(idempotencyKey, context.RequestAborted);
            _logger.LogWarning(ex, "幂等业务异常,释放标记: key={Key}", idempotencyKey);
            throw;  // 重新抛出,交给 ExceptionMiddleware 处理
        }
        finally
        {
            // 恢复原始响应流
            context.Response.Body = originalBodyStream;
        }
    }

    /// <summary>
    /// 把缓存的响应写回当前 HttpContext
    /// </summary>
    private static async Task WriteCachedResponseAsync(HttpContext context, IdempotentRecord record)
    {
        context.Response.StatusCode = record.StatusCode;
        context.Response.ContentType = record.ContentType ?? "application/json";

        // 添加幂等响应头,告知客户端本次响应来自缓存
        context.Response.Headers["X-Idempotent-Replay"] = "true";
        context.Response.Headers["X-Original-Timestamp"] = record.CreatedAt.ToString("O");

        if (!string.IsNullOrEmpty(record.ResponseBody))
        {
            var bytes = Encoding.UTF8.GetBytes(record.ResponseBody);
            await context.Response.Body.WriteAsync(bytes, context.RequestAborted);
        }
    }
}
