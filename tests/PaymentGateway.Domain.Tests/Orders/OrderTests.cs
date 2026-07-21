using PaymentGateway.Domain.Orders;
using PaymentGateway.Domain.Orders.Events;
using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using Xunit;

namespace PaymentGateway.Domain.Tests.Orders;

/// <summary>
/// 订单聚合根单元测试
/// 学习要点: 状态机测试重点验证
///   1. 合法状态转换成功
///   2. 非法状态转换抛异常
///   3. 工厂方法产生领域事件
/// </summary>
public class OrderTests
{
    // ========== 工厂方法测试 ==========

    [Fact]
    public void Create_WithValidInput_ShouldCreatePendingOrder()
    {
        var order = Order.Create(
            merchantId: 1,
            orderNo: "PG20250714001",
            outTradeNo: "OUT001",
            amount: Money.Yuan(99.99m),
            subject: "测试订单");

        Assert.Equal("PG20250714001", order.OrderNo);
        Assert.Equal(1, order.MerchantId);
        Assert.Equal(OrderStatus.Pending, order.Status);
        Assert.Equal(99.99m, order.Amount.Value);
        Assert.Null(order.PaidAt);
    }

    [Fact]
    public void Create_ShouldRaiseOrderCreatedEvent()
    {
        // 学习要点: 验证工厂方法是否正确产生领域事件
        // 领域事件在事务提交后由 DomainEventDispatcher 发布到 Kafka
        var order = Order.Create(1, "PG001", "OUT001", Money.Yuan(10m), "测试");

        Assert.Single(order.DomainEvents);
        Assert.IsType<OrderCreatedEvent>(order.DomainEvents.First());
    }

    [Theory]
    [InlineData(0)]         // 商户 ID 无效
    [InlineData(-1)]
    public void Create_WithInvalidMerchantId_ShouldThrowDomainException(long merchantId)
    {
        Assert.Throws<DomainException>(() =>
            Order.Create(merchantId, "PG001", "OUT001", Money.Yuan(10m), "测试"));
    }

    [Theory]
    [InlineData("", "OUT001")]
    [InlineData(null, "OUT001")]
    [InlineData("PG001", "")]
    [InlineData("PG001", null)]
    public void Create_WithEmptyOrderNoOrOutTradeNo_ShouldThrowDomainException(
        string orderNo, string outTradeNo)
    {
        Assert.Throws<DomainException>(() =>
            Order.Create(1, orderNo, outTradeNo, Money.Yuan(10m), "测试"));
    }

    [Fact]
    public void Create_WithZeroAmount_ShouldThrowDomainException()
    {
        Assert.Throws<DomainException>(() =>
            Order.Create(1, "PG001", "OUT001", Money.Yuan(0m), "测试"));
    }

    // ========== 状态机转换测试 ==========

    [Fact]
    public void MarkAsPaying_FromPending_ShouldTransitionToPaying()
    {
        var order = CreateTestOrder();

        order.MarkAsPaying("wechat", "channel_order_001");

        Assert.Equal(OrderStatus.Paying, order.Status);
        Assert.Equal("wechat", order.ChannelCode);
        Assert.Equal("channel_order_001", order.ChannelOrderNo);
    }

    [Fact]
    public void MarkAsPaid_FromPaying_ShouldTransitionToPaidAndSetPaidAt()
    {
        var order = CreateTestOrder();
        order.MarkAsPaying("wechat", "channel_order_001");

        order.MarkAsPaid();

        Assert.Equal(OrderStatus.Paid, order.Status);
        Assert.NotNull(order.PaidAt);
    }

    [Fact]
    public void MarkAsPaid_ShouldRaiseOrderPaidEvent()
    {
        var order = CreateTestOrder();
        order.MarkAsPaying("wechat", "channel_order_001");

        // 清空之前 OrderCreated 事件,只观察 MarkAsPaid 产生的事件
        order.ClearDomainEvents();

        order.MarkAsPaid();

        Assert.Single(order.DomainEvents);
        Assert.IsType<OrderPaidEvent>(order.DomainEvents.First());
    }

    [Fact]
    public void MarkAsPaying_FromPaid_ShouldThrowDomainException()
    {
        // 学习要点: 已支付订单不能再转回支付中(状态机不允许)
        var order = CreateTestOrder();
        order.MarkAsPaying("wechat", "channel_order_001");
        order.MarkAsPaid();

        Assert.Throws<DomainException>(() =>
            order.MarkAsPaying("alipay", "channel_order_002"));
    }

    [Fact]
    public void MarkAsPaid_FromPending_ShouldThrowDomainException()
    {
        // Pending 不能直接跳到 Paid,必须先经过 Paying
        var order = CreateTestOrder();

        Assert.Throws<DomainException>(() => order.MarkAsPaid());
    }

    [Fact]
    public void Close_FromPending_ShouldTransitionToClosed()
    {
        var order = CreateTestOrder();

        order.Close();

        Assert.Equal(OrderStatus.Closed, order.Status);
    }

    [Fact]
    public void Close_FromPaying_ShouldTransitionToClosed()
    {
        var order = CreateTestOrder();
        order.MarkAsPaying("wechat", "channel_order_001");

        order.Close();

        Assert.Equal(OrderStatus.Closed, order.Status);
    }

    [Fact]
    public void Close_FromPaid_ShouldThrowDomainException()
    {
        // 已支付订单不能关闭(应走退款流程)
        var order = CreateTestOrder();
        order.MarkAsPaying("wechat", "channel_order_001");
        order.MarkAsPaid();

        Assert.Throws<DomainException>(() => order.Close());
    }

    // ========== 状态机扩展方法测试 ==========

    [Theory]
    [InlineData(OrderStatus.Pending, OrderStatus.Paying, true)]
    [InlineData(OrderStatus.Paying, OrderStatus.Paid, true)]
    [InlineData(OrderStatus.Paid, OrderStatus.Refunded, true)]
    [InlineData(OrderStatus.Pending, OrderStatus.Closed, true)]
    [InlineData(OrderStatus.Paying, OrderStatus.Closed, true)]
    [InlineData(OrderStatus.Pending, OrderStatus.Paid, false)]    // 不能跳过 Paying
    [InlineData(OrderStatus.Paid, OrderStatus.Pending, false)]   // 不能回退
    [InlineData(OrderStatus.Closed, OrderStatus.Pending, false)] // 不能从关闭回到待支付
    public void CanTransitTo_ShouldReturnExpectedResult(
        OrderStatus from, OrderStatus to, bool expected)
    {
        Assert.Equal(expected, from.CanTransitTo(to));
    }

    // ========== 辅助方法 ==========

    private static Order CreateTestOrder()
    {
        return Order.Create(
            merchantId: 1,
            orderNo: "PG_TEST_001",
            outTradeNo: "OUT_TEST_001",
            amount: Money.Yuan(100m),
            subject: "测试订单");
    }
}
