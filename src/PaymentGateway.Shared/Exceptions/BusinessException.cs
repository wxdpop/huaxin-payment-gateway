namespace PaymentGateway.Shared.Exceptions;

/// <summary>
/// 业务异常 —— 用于表达可预期的业务规则违反(如余额不足、状态非法)
/// 学习要点: 业务异常 vs 系统异常
///   - 业务异常: 可预期,返回 400,用户可见(如"余额不足")
///   - 系统异常: 不可预期,返回 500,记录日志(如 NullReferenceException)
/// </summary>
public class BusinessException : Exception
{
    /// <summary>业务错误码,前端据此分支处理</summary>
    public string Code { get; }

    public BusinessException(string message, string code = "BIZ_ERROR")
        : base(message)
    {
        Code = code;
    }
}
