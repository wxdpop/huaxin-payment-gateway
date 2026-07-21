using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Domain.Shared;

/// <summary>
/// 金额值对象 —— 不可变(immutable),按属性值判断相等
/// 学习要点:
///   1. 金融场景金额绝不能用 float/double(精度丢失!),必须 decimal 或 NUMERIC
///   2. 用 record 自动实现值相等 + 不可变
///   3. 运算符重载使金额运算直观(如 money1 + money2)
///   4. Currency 校验防止不同币种直接相加
/// </summary>
public sealed record Money
{
    /// <summary>金额(单位:元,2位小数)</summary>
    public decimal Value { get; init; }

    /// <summary>币种(默认 CNY 人民币)</summary>
    public string Currency { get; init; } = "CNY";

    public Money(decimal value, string currency = "CNY")
    {
        if (value < 0) throw new DomainException("金额不能为负");
        Value = Math.Round(value, 2, MidpointRounding.ToEven);  // 银行家舍入,避免四舍五入偏差
        Currency = currency;
    }

    public static Money Zero => new(0m);
    public static Money Yuan(decimal value) => new(value);

    public static Money operator +(Money a, Money b)
    {
        EnsureSameCurrency(a, b);
        return new Money(a.Value + b.Value, a.Currency);
    }

    public static Money operator -(Money a, Money b)
    {
        EnsureSameCurrency(a, b);
        return new Money(a.Value - b.Value, a.Currency);
    }

    public static bool operator >(Money a, Money b)
    {
        EnsureSameCurrency(a, b);
        return a.Value > b.Value;
    }

    public static bool operator <(Money a, Money b)
    {
        EnsureSameCurrency(a, b);
        return a.Value < b.Value;
    }

    public static bool operator >=(Money a, Money b) => !(a < b);
    public static bool operator <=(Money a, Money b) => !(a > b);

    private static void EnsureSameCurrency(Money a, Money b)
    {
        if (!string.Equals(a.Currency, b.Currency, StringComparison.OrdinalIgnoreCase))
            throw new DomainException($"币种不一致: {a.Currency} vs {b.Currency}");
    }

    public override string ToString() => $"{Value:F2} {Currency}";
}
