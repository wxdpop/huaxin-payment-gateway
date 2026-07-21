namespace PaymentGateway.Shared.Exceptions;

/// <summary>
/// 并发冲突异常 —— 乐观锁版本号不匹配时抛出
/// 学习要点:
///   1. SqlSugar 5.x 没有 EFCore 那种 DbUpdateConcurrencyException 内置并发异常
///      需要自定义异常类型明确标识"乐观锁冲突",便于中间件统一映射为 HTTP 409
///   2. 不要直接用 InvalidOperationException:
///      该类型是 BCL 通用异常,会被其他业务场景共用,无法精确匹配乐观锁场景
///   3. 此异常由 Repository.Update 在 WHERE 影响行数=0 时抛出
///      应用层可捕获后重试(如指数退避重试 3 次)
/// </summary>
public class ConcurrencyException : Exception
{
    /// <summary>冲突的资源类型(如 "账户")</summary>
    public string ResourceType { get; }

    /// <summary>冲突的资源ID</summary>
    public string ResourceId { get; }

    public ConcurrencyException(string resourceType, string resourceId)
        : base($"乐观锁冲突: {resourceType} {resourceId} 已被其他事务更新,请重试")
    {
        ResourceType = resourceType;
        ResourceId = resourceId;
    }

    public ConcurrencyException(string resourceType, long resourceId)
        : this(resourceType, resourceId.ToString()) { }
}
