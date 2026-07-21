namespace PaymentGateway.Application.Abstractions;

// ============================================================================
// 事件总线抽象层 (IEventBus + IIntegrationEvent + 消费者接口)
// ============================================================================
// ★ 学习要点: 与 IDistributedLock 一样,抽象定义在 Application 层
//   - Application 层的 HandleCallbackService 需要发布事件
//   - Infrastructure 层的 KafkaEventBus 实现此接口
//   - 避免循环依赖: Application 不引用 Infrastructure
// ============================================================================

public interface IIntegrationEvent
{
    string EventId { get; }
    DateTimeOffset OccurredAt { get; }
    string EventType { get; }
}

public interface IEventBus
{
    Task PublishAsync<TEvent>(
        string topic,
        TEvent @event,
        CancellationToken ct = default) where TEvent : IIntegrationEvent;
}

public interface IIntegrationEventConsumer<in TEvent> where TEvent : IIntegrationEvent
{
    string Topic { get; }
    string ConsumerGroup { get; }
    Task<bool> HandleAsync(TEvent @event, CancellationToken ct = default);
}

/// <summary>
/// 事件总线配置选项 —— 通过 appsettings.json "EventBus" 节绑定
/// 学习要点: 同一份配置同时驱动 Kafka 生产者与消费者
///   - 生产者用 BootstrapServers/ProducerAcks/MessageTimeoutMs
///   - 消费者用 DefaultConsumerGroup/SessionTimeoutMs/AutoCommitIntervalMs/MaxRetryCount
/// </summary>
public class EventBusOptions
{
    /// <summary>Kafka Broker 地址列表 (如 localhost:29092 或 kafka:9092)</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>默认消费者组名 (同组内负载均衡,不同组各自消费全量)</summary>
    public string DefaultConsumerGroup { get; set; } = "payment-gateway";

    /// <summary>
    /// 生产者确认级别: "all"(默认,Leader+全部副本确认,最安全) / "1"(Leader确认) / "0"(不等确认)
    /// 学习要点: 资金场景必须用 "all",避免 Leader 宕机时消息丢失
    /// </summary>
    public string ProducerAcks { get; set; } = "all";

    /// <summary>生产者消息发送超时(毫秒,默认 10s) — 超时未确认则抛异常</summary>
    public int MessageTimeoutMs { get; set; } = 10_000;

    /// <summary>消费者会话超时(毫秒,默认 30s) — 超时未心跳触发 Rebalance</summary>
    public int SessionTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// 自动提交间隔(毫秒,默认 0=禁用自动提交,改手动提交)
    /// 学习要点: 资金场景必须手动提交 (At-Least-Once 语义),避免消费成功未提交时宕机丢消息
    /// </summary>
    public int AutoCommitIntervalMs { get; set; } = 0;

    /// <summary>消费失败最大重试次数(默认 3) — 超过后转入死信队列</summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// 是否使用内存事件总线 (true=进程内同步调用, false=Kafka 异步驱动)
    /// 学习要点:
    ///   - 本地开发用 true (免依赖 Kafka,快速验证业务链路)
    ///   - 容器化联调用 false (真实事件驱动,验证消费者与重试机制)
    /// </summary>
    public bool UseInMemory { get; set; } = false;
}
