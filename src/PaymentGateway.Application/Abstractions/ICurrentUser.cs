namespace PaymentGateway.Application.Abstractions;

/// <summary>
/// 当前用户上下文 —— 抽象当前请求的用户信息
/// 学习要点:
///   1. 不直接依赖 HttpContext,便于应用层独立测试(无需 Mock HttpContext)
///   2. Api 层通过 CurrentUserMiddleware 从 JWT/请求头解析后注入
///   3. 后续接入网关鉴权时,从商户Token中解析 MerchantId
/// </summary>
public interface ICurrentUser
{
    long? MerchantId { get; }

    /// <summary>请求追踪ID,关联 Jaeger 链路</summary>
    string? TraceId { get; }
}

/// <summary>默认实现(无鉴权场景使用,如内部测试)</summary>
internal sealed class CurrentUser : ICurrentUser
{
    public long? MerchantId { get; set; }
    public string? TraceId { get; set; }
}
