using PaymentGateway.Application.Abstractions;

namespace PaymentGateway.Application.EventBus;

// ============================================================================
// 支付网关集成事件定义 (Application 层契约)
// ============================================================================
// 放在 Application 层的原因:
//   - 集成事件是业务契约,由应用层定义
//   - Infrastructure 层的 KafkaEventBus 负责序列化和传输
//   - Api 层的 Consumers 负责反序列化和消费
// ============================================================================

public record CallbackReceivedEvent : IIntegrationEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
    public string EventType => nameof(CallbackReceivedEvent);

    public string OrderNo { get; init; } = string.Empty;
    public string ChannelOrderNo { get; init; } = string.Empty;
    public string ChannelTradeNo { get; init; } = string.Empty;
    public string ChannelCode { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public DateTimeOffset PaidAt { get; init; }
    public string CallbackRaw { get; init; } = string.Empty;
}

public record PaymentSucceededEvent : IIntegrationEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
    public string EventType => nameof(PaymentSucceededEvent);

    public long OrderId { get; init; }
    public string OrderNo { get; init; } = string.Empty;
    public long MerchantId { get; init; }
    public decimal Amount { get; init; }
    public string ChannelCode { get; init; } = string.Empty;
    public string ChannelOrderNo { get; init; } = string.Empty;
}

public record AccountCreditedEvent : IIntegrationEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
    public string EventType => nameof(AccountCreditedEvent);

    public long AccountId { get; init; }
    public long MerchantId { get; init; }
    public long OrderId { get; init; }
    public string OrderNo { get; init; } = string.Empty;
    public decimal Amount { get; init; }
    public decimal BalanceAfter { get; init; }
    public string BizNo { get; init; } = string.Empty;
}

public record MerchantNotifyEvent : IIntegrationEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
    public string EventType => nameof(MerchantNotifyEvent);

    public long MerchantId { get; init; }
    public string OrderNo { get; init; } = string.Empty;
    public string NotifyUrl { get; init; } = string.Empty;
    public string Payload { get; init; } = string.Empty;
    public int RetryCount { get; init; }
}

/// <summary>退款成功事件(退款完成后发布,触发商户通知与对账)</summary>
public record RefundSucceededEvent : IIntegrationEvent
{
    public string EventId { get; init; } = Guid.NewGuid().ToString("N");
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
    public string EventType => nameof(RefundSucceededEvent);

    public long OrderId { get; init; }
    public string OrderNo { get; init; } = string.Empty;
    public long MerchantId { get; init; }
    public string RefundNo { get; init; } = string.Empty;
    public decimal Amount { get; init; }
}
