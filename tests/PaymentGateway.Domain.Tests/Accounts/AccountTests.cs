using PaymentGateway.Domain.Accounts;
using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using Xunit;

namespace PaymentGateway.Domain.Tests.Accounts;

/// <summary>
/// 资金账户聚合根单元测试
/// 学习要点: 聚合根的测试重点验证
///   1. 业务方法的不变量(余额不能为负,扣款必须足够)
///   2. 业务方法只修改业务字段,不自增乐观锁版本号(版本号由 AccountRepository.Update 负责)
///   3. 工厂方法是否正确初始化聚合根状态
/// </summary>
public class AccountTests
{
    // ========== 工厂方法测试 ==========

    [Fact]
    public void Create_WithValidInput_ShouldInitializeCorrectly()
    {
        // Arrange
        var merchantId = 1L;
        var initialBalance = Money.Yuan(1000m);

        // Act
        var account = Account.Create(merchantId, initialBalance);

        // Assert
        Assert.Equal(merchantId, account.MerchantId);
        Assert.Equal(1000m, account.Balance.Value);
        Assert.Equal(0m, account.FrozenAmount.Value);
        Assert.Equal(0, account.Version);
    }

    [Fact]
    public void Create_WithInvalidMerchantId_ShouldThrowDomainException()
    {
        Assert.Throws<DomainException>(() => Account.Create(0, Money.Yuan(100m)));
        Assert.Throws<DomainException>(() => Account.Create(-1, Money.Yuan(100m)));
    }

    // ========== Credit 入账测试 ==========

    [Fact]
    public void Credit_WithPositiveAmount_ShouldIncreaseBalance()
    {
        // Arrange
        var account = Account.Create(1, Money.Yuan(1000m));

        // Act
        account.Credit(Money.Yuan(500m));

        // Assert
        Assert.Equal(1500m, account.Balance.Value);
        // 学习要点: 业务方法不自增 Version,由 AccountRepository.Update 负责
        Assert.Equal(0, account.Version);
    }

    [Fact]
    public void Credit_WithZeroOrNegativeAmount_ShouldThrowDomainException()
    {
        var account = Account.Create(1, Money.Yuan(1000m));

        Assert.Throws<DomainException>(() => account.Credit(Money.Yuan(0m)));
        // 测试负数: 构造 Money 时就会抛异常
        Assert.Throws<DomainException>(() => account.Credit(new Money(-1m)));
    }

    // ========== Debit 出账测试 ==========

    [Fact]
    public void Debit_WithSufficientBalance_ShouldDecreaseBalance()
    {
        var account = Account.Create(1, Money.Yuan(1000m));

        account.Debit(Money.Yuan(300m));

        Assert.Equal(700m, account.Balance.Value);
        // 学习要点: 业务方法不自增 Version,由 AccountRepository.Update 负责
        Assert.Equal(0, account.Version);
    }

    [Fact]
    public void Debit_WithInsufficientBalance_ShouldThrowDomainException()
    {
        var account = Account.Create(1, Money.Yuan(100m));

        var ex = Assert.Throws<DomainException>(() => account.Debit(Money.Yuan(200m)));
        Assert.Contains("余额不足", ex.Message);
    }

    [Fact]
    public void Debit_WithZeroAmount_ShouldThrowDomainException()
    {
        var account = Account.Create(1, Money.Yuan(100m));
        Assert.Throws<DomainException>(() => account.Debit(Money.Yuan(0m)));
    }

    // ========== Freeze 冻结测试 ==========

    [Fact]
    public void Freeze_WithSufficientBalance_ShouldMoveAmountToFrozen()
    {
        var account = Account.Create(1, Money.Yuan(1000m));

        account.Freeze(Money.Yuan(300m));

        // 学习要点: 冻结操作是"可用余额→冻结金额"的转移,总额不变
        Assert.Equal(700m, account.Balance.Value);
        Assert.Equal(300m, account.FrozenAmount.Value);
        // 学习要点: 业务方法不自增 Version,由 AccountRepository.Update 负责
        Assert.Equal(0, account.Version);
    }

    [Fact]
    public void Freeze_WithInsufficientBalance_ShouldThrowDomainException()
    {
        var account = Account.Create(1, Money.Yuan(100m));

        var ex = Assert.Throws<DomainException>(() => account.Freeze(Money.Yuan(200m)));
        Assert.Contains("可用余额不足", ex.Message);
    }

    // ========== Unfreeze 解冻测试 ==========

    [Fact]
    public void Unfreeze_WithSufficientFrozen_ShouldMoveAmountBackToBalance()
    {
        var account = Account.Create(1, Money.Yuan(1000m));
        account.Freeze(Money.Yuan(300m));  // 先冻结 300

        account.Unfreeze(Money.Yuan(300m));

        // 学习要点: 解冻是"冻结金额→可用余额"的转移,总额不变
        Assert.Equal(1000m, account.Balance.Value);
        Assert.Equal(0m, account.FrozenAmount.Value);
        // 学习要点: 业务方法不自增 Version,由 AccountRepository.Update 负责
        Assert.Equal(0, account.Version);
    }

    [Fact]
    public void Unfreeze_WithInsufficientFrozen_ShouldThrowDomainException()
    {
        var account = Account.Create(1, Money.Yuan(1000m));
        account.Freeze(Money.Yuan(100m));  // 只冻结 100

        var ex = Assert.Throws<DomainException>(() => account.Unfreeze(Money.Yuan(200m)));
        Assert.Contains("冻结金额不足", ex.Message);
    }

    // ========== 复合场景测试 ==========

    [Fact]
    public void CompositeScenario_CreditThenDebitThenFreeze_ShouldMaintainCorrectBalance()
    {
        // 学习要点: 模拟真实业务流程(入账→扣款→冻结),验证金额一致性
        var account = Account.Create(1, Money.Yuan(0m));

        // 1. 充值 1000
        account.Credit(Money.Yuan(1000m));
        Assert.Equal(1000m, account.Balance.Value);

        // 2. 消费 300
        account.Debit(Money.Yuan(300m));
        Assert.Equal(700m, account.Balance.Value);

        // 3. 退款预冻结 200(从可用余额转入冻结)
        account.Freeze(Money.Yuan(200m));
        Assert.Equal(500m, account.Balance.Value);
        Assert.Equal(200m, account.FrozenAmount.Value);

        // 4. 退款失败,解冻 200
        account.Unfreeze(Money.Yuan(200m));
        Assert.Equal(700m, account.Balance.Value);
        Assert.Equal(0m, account.FrozenAmount.Value);

        // 学习要点: 业务方法不自增 Version,连续调用多次后 Version 仍为 0
        //   乐观锁版本号由 AccountRepository.Update 在持久化时统一自增
        Assert.Equal(0, account.Version);
    }
}
