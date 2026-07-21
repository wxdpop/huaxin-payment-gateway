using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Application.EventBus;
using PaymentGateway.Infrastructure.EventBus;

namespace PaymentGateway.Api.Consumers;

// ============================================================================
// MerchantNotifyConsumer —— 商户通知消费者 (Api 层宿主)
// ============================================================================
// ★ 学习要点: 商户通知是"最终一致性"的体现
//   - 支付成功 → 入账 → 通知商户 (异步解耦)
//   - 通知失败 → 重试 → 死信队列 (人工补偿)
//
// 【通知重试策略】
//   1. Kafka 消费者内置指数退避重试 (基类 KafkaConsumerService 已实现)
//   2. 超过重试次数 → 进入死信队列
//   3. 死信队列由补偿任务定期扫描重发
//
// 【HTTP 通知的幂等设计】
//   - 商户侧应实现幂等 (通过 orderNo 判断是否已处理)
//   - 通知 Payload 包含支付结果 + 签名 (商户验签)
// ============================================================================

public class MerchantNotifyConsumer : KafkaConsumerService<AccountCreditedEvent>
{
    private readonly ILogger<MerchantNotifyConsumer> _logger;

    public MerchantNotifyConsumer(
        IServiceProvider serviceProvider,
        IOptions<EventBusOptions> options,
        ILogger<MerchantNotifyConsumer> logger)
        : base(options, logger)
    {
        _logger = logger;
    }

    protected override string Topic => PaymentEventTopics.AccountCredited;
    protected override string ConsumerGroup => "payment-merchant-notify";

    /// <summary>
    /// 处理账户入账事件 (公开入口,供内存模式 InMemoryEventBus 订阅调用)
    /// 学习要点: 与 CreditAccountConsumer 同理,内存模式下通过订阅直接调用
    /// </summary>
    public Task<bool> HandleAccountCreditedAsync(AccountCreditedEvent @event, CancellationToken ct)
        => HandleAsync(@event, ct);

    protected override async Task<bool> HandleAsync(AccountCreditedEvent @event, CancellationToken ct)
    {
        _logger.LogInformation(
            "商户通知开始: orderNo={OrderNo}, merchantId={MerchantId}, amount={Amount}",
            @event.OrderNo, @event.MerchantId, @event.Amount);

        try
        {
            // ★ 学习要点: HTTP 通知商户
            //   - 真实场景: 通过 HttpClient POST 到商户配置的 notify_url
            //   - 本工程学习简化: 仅记录日志,不实际发送
            //   - 生产代码应注入 IHttpClientFactory + Polly 重试策略
            //
            // 伪代码 (注释说明,不实际调用):
            //   var payload = JsonSerializer.Serialize(new {
            //       order_no = @event.OrderNo,
            //       amount = @event.Amount,
            //       status = "PAID",
            //       timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            //       sign = SignPayload(...)  // 签名
            //   });
            //   var response = await _httpClient.PostAsync(notifyUrl, new StringContent(payload));
            //   if (response.IsSuccessStatusCode) return true;

            await Task.Delay(100, ct);  // 模拟 HTTP 调用耗时

            _logger.LogInformation(
                "商户通知成功(模拟): orderNo={OrderNo}, merchantId={MerchantId}",
                @event.OrderNo, @event.MerchantId);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "商户通知失败: orderNo={OrderNo}", @event.OrderNo);
            return false;  // 返回 false 触发基类重试机制
        }
    }
}
