using PaymentGateway.Shared.Exceptions;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Middleware;

/// <summary>
/// 全局异常处理中间件 —— 统一异常到 Result 结构的转换
/// 学习要点:
///   1. ASP.NET Core 中间件管道: UseMiddleware 顺序执行,异常向上抛
///   2. 用 try/catch 包裹 next() 捕获所有异常,转换为统一响应结构
///   3. 不同异常类型映射不同 HTTP 状态码:
///      - BusinessException: 400(可预期业务错误,用户可见)
///      - DomainException: 400(领域规则违反)
///      - ConcurrencyException: 409(乐观锁冲突,可重试)
///        SqlSugar 5.x 无内置并发异常,自定义 ConcurrencyException 精确标识
///      - 其他: 500(系统异常,记录日志)
///   4. TraceId 关联日志与 Jaeger 链路,便于排查
/// </summary>
public class ExceptionMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ExceptionMiddleware> _logger;

    public ExceptionMiddleware(RequestDelegate next, ILogger<ExceptionMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(context, ex);
        }
    }

    private async Task HandleExceptionAsync(HttpContext context, Exception ex)
    {
        var traceId = context.TraceIdentifier;
        (int statusCode, Result result) = ex switch
        {
            Shared.Exceptions.BusinessException b => (StatusCodes.Status400BadRequest,
                Result.Fail(b.Message, b.Code)),
            Shared.Exceptions.DomainException d => (StatusCodes.Status400BadRequest,
                Result.Fail(d.Message, "DOMAIN_ERROR")),
            ConcurrencyException c => (StatusCodes.Status409Conflict,
                Result.Fail(c.Message, "CONFLICT")),
            _ => (StatusCodes.Status500InternalServerError,
                Result.Fail("系统内部错误,请联系管理员", "SYSTEM_ERROR"))
        };

        // 系统异常记录详细日志(含堆栈),业务异常仅记录警告
        if (statusCode >= 500)
            _logger.LogError(ex, "系统异常: TraceId={TraceId}, Path={Path}", traceId, context.Request.Path);
        else
            _logger.LogWarning(ex, "业务异常: TraceId={TraceId}, Path={Path}", traceId, context.Request.Path);

        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json";

        // 把 TraceId 注入到响应体(便于前端关联日志)
        // 学习要点: Result 是 class 不是 record,不能用 with 表达式,需重新构造
        var responseWithTrace = new Result
        {
            Success = result.Success,
            Code = result.Code,
            Message = result.Message,
            TraceId = traceId
        };
        await System.Text.Json.JsonSerializer.SerializeAsync(
            context.Response.Body, responseWithTrace,
            cancellationToken: context.RequestAborted);
    }
}
