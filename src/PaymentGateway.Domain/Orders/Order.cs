using PaymentGateway.Domain.Orders.Events;
using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;
using SqlSugar;

namespace PaymentGateway.Domain.Orders;

/// <summary>
/// 订单聚合根 —— 支付交易的核心实体
/// 学习要点: 聚合根封装业务规则,外部不能直接 set 属性
///   - 所有状态变更必须通过业务方法(MarkAsPaying/MarkAsPaid)
///   - 方法内部校验状态机合法性,非法转换抛 DomainException
///   - 工厂方法 Create 保证创建时不变量成立
/// SqlSugar 映射说明:
///   - [SugarTable] 指定表名
///   - [SugarColumn(ColumnName=...)] 指定列名(默认按属性名映射)
///   - Money 值对象用"双属性方案": Amount(Money) + AmountValue(decimal 持久化)
///     Amount 加 [SugarColumn(IsIgnore=true)] 让 SqlSugar 忽略,持久化通过 AmountValue
/// </summary>
[SugarTable("orders")]
public class Order : AggregateRoot<long>
{
    /// <summary>平台订单号(全局唯一)</summary>
    [SugarColumn(ColumnName = "order_no", Length = 32, IsNullable = false)]
    public string OrderNo { get; private set; } = string.Empty;

    /// <summary>商户ID</summary>
    [SugarColumn(ColumnName = "merchant_id")]
    public long MerchantId { get; private set; }

    /// <summary>商户订单号(商户系统内部唯一)</summary>
    [SugarColumn(ColumnName = "out_trade_no", Length = 64, IsNullable = false)]
    public string OutTradeNo { get; private set; } = string.Empty;

    /// <summary>路由后选定的支付渠道(wechat/alipay/unionpay)</summary>
    [SugarColumn(ColumnName = "channel_code", Length = 32, IsNullable = true)]
    public string? ChannelCode { get; private set; }

    /// <summary>渠道侧订单号(渠道返回)</summary>
    [SugarColumn(ColumnName = "channel_order_no", Length = 64, IsNullable = true)]
    public string? ChannelOrderNo { get; private set; }

    /// <summary>订单标题</summary>
    [SugarColumn(ColumnName = "subject", Length = 256, IsNullable = true)]
    public string Subject { get; private set; } = string.Empty;

    /// <summary>持久化字段(decimal 直接映射 MySQL DECIMAL 类型)</summary>
    [SugarColumn(ColumnName = "amount", ColumnDataType = "decimal(18,2)")]
    public decimal AmountValue { get; private set; }

    /// <summary>金额(值对象) —— 不持久化,通过 AmountValue 读写</summary>
    [SugarColumn(IsIgnore = true)]
    public Money Amount
    {
        get => Money.Yuan(AmountValue);
        private set => AmountValue = value.Value;
    }

    /// <summary>状态(0待支付 1支付中 2已支付 3已退款 4已关闭)</summary>
    [SugarColumn(ColumnName = "status", ColumnDataType = "smallint")]
    public OrderStatus Status { get; private set; }

    [SugarColumn(ColumnName = "created_at")]
    public DateTimeOffset CreatedAt { get; private set; }

    [SugarColumn(ColumnName = "paid_at", IsNullable = true)]
    public DateTimeOffset? PaidAt { get; private set; }

    [SugarColumn(ColumnName = "updated_at")]
    public DateTimeOffset UpdatedAt { get; private set; }

    // 学习要点: DomainEvents 由基类 AggregateRoot 统一声明 [SugarColumn(IsIgnore=true)]
    //   子类无需重复声明,直接继承使用即可

    // SqlSugar 5.x 的 Insertable<T>/Updateable<T> 有 where T : class, new() 约束
    // 学习要点: ORM 与 DDD 的常见取舍 - ORM 需要反射创建实例,要求 public 无参构造函数
    //   严格 DDD 倾向 protected/private 构造函数(强制走工厂方法),
    //   但 SqlSugar 等主流 ORM 要求 public,这里遵循 ORM 约定
    //   实际项目中可通过 [SugarColumn(IsIgnore=true)] + 内部构造函数绕过,但增加复杂度
    public Order() { }

    /// <summary>
    /// 工厂方法 —— 创建订单,保证不变量(金额>0,订单号非空)
    /// 学习要点: 用工厂方法而非构造函数,集中校验逻辑
    /// </summary>
    public static Order Create(
        long merchantId,
        string orderNo,
        string outTradeNo,
        Money amount,
        string subject)
    {
        if (merchantId <= 0) throw new DomainException("商户ID无效");
        if (string.IsNullOrWhiteSpace(orderNo)) throw new DomainException("订单号不能为空");
        if (string.IsNullOrWhiteSpace(outTradeNo)) throw new DomainException("商户订单号不能为空");
        if (amount.Value <= 0) throw new DomainException("订单金额必须大于0");

        var now = DateTimeOffset.UtcNow;
        var order = new Order
        {
            MerchantId = merchantId,
            OrderNo = orderNo,
            OutTradeNo = outTradeNo,
            Amount = amount,
            Subject = subject,
            Status = OrderStatus.Pending,
            CreatedAt = now,
            UpdatedAt = now
        };

        // ★ 产生领域事件(应用层在事务提交后发布到 Kafka)
        order.AddDomainEvent(new OrderCreatedEvent(order.Id, order.OrderNo, order.MerchantId));
        return order;
    }

    /// <summary>标记为支付中(已向渠道发起支付)</summary>
    public void MarkAsPaying(string channelCode, string channelOrderNo)
    {
        EnsureCanTransit(OrderStatus.Paying);
        ChannelCode = channelCode ?? throw new DomainException("渠道编码不能为空");
        ChannelOrderNo = channelOrderNo ?? throw new DomainException("渠道订单号不能为空");
        Status = OrderStatus.Paying;
        UpdatedAt = DateTimeOffset.UtcNow;
    }

    /// <summary>标记为已支付(收到渠道成功回调)</summary>
    public void MarkAsPaid()
    {
        EnsureCanTransit(OrderStatus.Paid);
        Status = OrderStatus.Paid;
        PaidAt = DateTimeOffset.UtcNow;
        UpdatedAt = PaidAt.Value;
        AddDomainEvent(new OrderPaidEvent(Id, OrderNo, MerchantId, Amount.Value));
    }

    /// <summary>关闭订单(超时未支付)</summary>
    public void Close()
    {
        EnsureCanTransit(OrderStatus.Closed);
        Status = OrderStatus.Closed;
        UpdatedAt = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// 标记为已退款(退款成功后调用)
    /// 学习要点: 状态机保证只有 Paid 状态的订单才能退款
    ///   退款后订单不可逆,不能再次支付或退款
    /// </summary>
    public void MarkAsRefunded()
    {
        EnsureCanTransit(OrderStatus.Refunded);
        Status = OrderStatus.Refunded;
        UpdatedAt = DateTimeOffset.UtcNow;
    }

    private void EnsureCanTransit(OrderStatus target)
    {
        if (!Status.CanTransitTo(target))
            throw new DomainException($"订单状态 {Status} 不允许转换为 {target}");
    }
}
