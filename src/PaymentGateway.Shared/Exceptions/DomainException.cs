namespace PaymentGateway.Shared.Exceptions;

/// <summary>
/// 领域异常 —— 领域层不变量(invariant)被违反时抛出
/// 学习要点: DDD 中领域异常应从领域层抛出,而非应用层
///   例如 Order.MarkAsPaid() 时状态不是 Paying,应抛 DomainException
///   这样领域规则被封装在聚合根内,外部无法绕过
/// </summary>
public class DomainException : Exception
{
    public DomainException(string message) : base(message) { }
}
