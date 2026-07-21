using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using Xunit;

namespace PaymentGateway.Domain.Tests.Shared;

/// <summary>
/// Money 值对象单元测试
/// 学习要点: 单元测试应覆盖三类场景
///   1. 正常路径(Happy Path): 合法输入产生预期结果
///   2. 边界条件: 0 值/大数/精度边界
///   3. 异常路径: 非法输入抛出预期异常
/// </summary>
public class MoneyTests
{
    // ========== 构造与工厂方法测试 ==========

    [Fact]
    public void Constructor_WithPositiveValue_ShouldCreateInstance()
    {
        // Arrange & Act
        var money = new Money(100.50m);

        // Assert
        Assert.Equal(100.50m, money.Value);
        Assert.Equal("CNY", money.Currency);
    }

    [Fact]
    public void Constructor_WithNegativeValue_ShouldThrowDomainException()
    {
        // Arrange & Act & Assert
        Assert.Throws<DomainException>(() => new Money(-1m));
    }

    [Fact]
    public void Constructor_WithZero_ShouldCreateZeroMoney()
    {
        var money = new Money(0m);
        Assert.Equal(0m, money.Value);
    }

    [Fact]
    public void Yuan_Factory_ShouldCreateMoneyWithCNY()
    {
        var money = Money.Yuan(99.99m);
        Assert.Equal(99.99m, money.Value);
        Assert.Equal("CNY", money.Currency);
    }

    [Fact]
    public void Zero_ShouldReturnZeroMoney()
    {
        var zero = Money.Zero;
        Assert.Equal(0m, zero.Value);
    }

    // ========== 精度处理测试 ==========

    [Theory]
    [InlineData(100.125, 100.12)]   // 银行家舍入: 2 舍
    [InlineData(100.135, 100.14)]   // 银行家舍入: 4 入(偶数舍入到偶数)
    [InlineData(100.145, 100.14)]   // 银行家舍入: 4 舍(1 是奇数,舍到偶数)
    [InlineData(100.155, 100.16)]   // 银行家舍入: 5 入(到偶数)
    public void Constructor_ShouldUseBankersRounding(decimal input, decimal expected)
    {
        // 学习要点: 金融场景必须用 MidpointRounding.ToEven(银行家舍入)
        //   避免传统四舍五入的统计偏差(多次舍入后总额偏大)
        var money = new Money(input);
        Assert.Equal(expected, money.Value);
    }

    // ========== 运算符重载测试 ==========

    [Fact]
    public void OperatorAdd_SameCurrency_ShouldReturnSum()
    {
        var a = Money.Yuan(100m);
        var b = Money.Yuan(50.50m);

        var result = a + b;

        Assert.Equal(150.50m, result.Value);
        Assert.Equal("CNY", result.Currency);
    }

    [Fact]
    public void OperatorSubtract_SameCurrency_ShouldReturnDifference()
    {
        var a = Money.Yuan(100m);
        var b = Money.Yuan(30.50m);

        var result = a - b;

        Assert.Equal(69.50m, result.Value);
    }

    [Fact]
    public void OperatorAdd_DifferentCurrency_ShouldThrowDomainException()
    {
        var a = new Money(100m, "CNY");
        var b = new Money(10m, "USD");

        Assert.Throws<DomainException>(() => a + b);
    }

    [Fact]
    public void OperatorGreaterThan_SameCurrency_ShouldCompareCorrectly()
    {
        var small = Money.Yuan(10m);
        var large = Money.Yuan(100m);

        Assert.True(large > small);
        Assert.False(small > large);
        Assert.True(small < large);
        Assert.True(large >= small);
        Assert.True(small <= large);
    }

    [Fact]
    public void OperatorCompare_DifferentCurrency_ShouldThrowDomainException()
    {
        var a = new Money(100m, "CNY");
        var b = new Money(50m, "USD");

        Assert.Throws<DomainException>(() => a > b);
    }

    // ========== ToString 测试 ==========

    [Fact]
    public void ToString_ShouldFormatWithTwoDecimals()
    {
        var money = Money.Yuan(99.5m);
        Assert.Equal("99.50 CNY", money.ToString());
    }

    // ========== 值相等测试 ==========

    [Fact]
    public void Equals_SameValueAndCurrency_ShouldBeEqual()
    {
        // 学习要点: record 自动按属性值实现相等性
        var a = Money.Yuan(100m);
        var b = Money.Yuan(100m);

        Assert.Equal(a, b);
        Assert.True(a == b);
    }

    [Fact]
    public void Equals_DifferentValue_ShouldNotBeEqual()
    {
        var a = Money.Yuan(100m);
        var b = Money.Yuan(99m);

        Assert.NotEqual(a, b);
        Assert.True(a != b);
    }
}
