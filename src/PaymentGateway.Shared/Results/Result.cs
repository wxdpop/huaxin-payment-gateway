namespace PaymentGateway.Shared.Results;

/// <summary>
/// 统一返回结构 —— 所有 API 响应使用此结构,保证前端可统一处理
/// 学习要点: 用泛型 Result&lt;T&gt; 而非抛异常表达业务失败,避免异常滥用
/// </summary>
public class Result
{
    public bool Success { get; init; }
    public string? Code { get; init; }       // "OK" / "BIZ_001" 等业务码
    public string? Message { get; init; }
    public string? TraceId { get; init; }    // 关联 Jaeger 链路,便于排查

    public static Result Ok() => new() { Success = true, Code = "OK" };
    public static Result Fail(string message, string code = "FAIL") =>
        new() { Success = false, Code = code, Message = message };
}

/// <summary>
/// 带数据的返回结果
/// </summary>
public class Result<T> : Result
{
    public T? Data { get; init; }

    public static Result<T> Ok(T data) => new() { Success = true, Code = "OK", Data = data };
    public static new Result<T> Fail(string message, string code = "FAIL") =>
        new() { Success = false, Code = code, Message = message };
}
