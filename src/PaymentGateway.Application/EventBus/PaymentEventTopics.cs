namespace PaymentGateway.Application.EventBus;

// ============================================================================
// Kafka 主题常量 (Application 层契约)
// ============================================================================

public static class PaymentEventTopics
{
    /// <summary>渠道回调到达 (Api 层接收渠道回调后发布,触发后续幂等校验链路)</summary>
    public const string CallbackReceived = "payment.callback.received";

    /// <summary>支付成功 (回调处理完成且订单已置为 Paid 后发布,驱动账户入账)</summary>
    public const string PaymentSucceeded = "payment.succeeded";

    /// <summary>支付失败 (渠道回调失败或超时关单后发布,驱动订单关闭与商户通知)</summary>
    public const string PaymentFailed = "payment.failed";

    /// <summary>账户入账成功 (CreditAccountConsumer 入账完成后发布,驱动商户通知)</summary>
    public const string AccountCredited = "account.credited";

    /// <summary>账户扣款成功 (退款扣减余额后发布,驱动退款完成通知与对账)</summary>
    public const string AccountDebited = "account.debited";

    /// <summary>商户异步通知 (通知商户支付/退款结果,失败重试达上限进死信队列)</summary>
    public const string MerchantNotify = "merchant.notify";

    /// <summary>退款申请发起 (商户发起退款后发布,驱动退款处理与渠道调用)</summary>
    public const string RefundRequested = "refund.requested";

    /// <summary>死信队列 (消费失败超过 MaxRetryCount 后转入,人工介入或补偿任务处理)</summary>
    public const string DeadLetterQueue = "payment.dlq";

    /// <summary>全部 Topic 列表 (应用启动时可用于批量创建 Topic,避免首次发送时自动创建的延迟)</summary>
    public static readonly string[] AllTopics =
    {
        CallbackReceived, PaymentSucceeded, PaymentFailed,
        AccountCredited, AccountDebited,
        MerchantNotify, RefundRequested, DeadLetterQueue
    };
}
