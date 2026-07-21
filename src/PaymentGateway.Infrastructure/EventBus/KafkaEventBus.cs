using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PaymentGateway.Application.Abstractions;

namespace PaymentGateway.Infrastructure.EventBus;

// ============================================================================
// KafkaEventBus —— Kafka 生产者实现
// ============================================================================
// ★ 学习要点: Kafka 生产者核心概念
//
// 【acks 配置】(消息持久化保证级别)
//   - acks=0:   生产者不等待任何确认 (可能丢失,最高吞吐)
//   - acks=1:   等待 Leader 副本确认 (Leader 宕机可能丢失)
//   - acks=all: 等待所有 ISR 副本确认 (最强持久化,默认推荐)
//
// 【Outbox 模式 vs 直接发送】
//   - 直接发送: 业务事务 + 消息发送分离 → 可能事务成功但消息丢失 (网络抖动)
//   - Outbox 模式: 业务事务内写 outbox 表 → 后台任务异步发送 → 保证不丢消息
//   本工程简化为"直接发送",生产环境应实现 Outbox (M4 阶段考虑)
//
// 【消息 Key 设计】
//   - Key 决定消息路由到哪个 Partition (相同 Key 总到同一 Partition)
//   - 本工程用 OrderNo 作 Key: 同一订单的事件按顺序消费 (FIFO)
//   - 学习要点: Kafka 只保证单 Partition 内有序,跨 Partition 无序
//
// 【幂等生产者 (Idempotent Producer)】
//   - EnableIdempotence=true: 防止网络重试导致重复消息
//   - Kafka 自动去重: PID + SequenceNumber 机制
//   - 本工程启用,与 acks=all 配合实现 Exactly-Once 语义 (单分区场景)
// ============================================================================

public class KafkaEventBus : IEventBus, IDisposable, IAsyncDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly EventBusOptions _options;
    private readonly ILogger<KafkaEventBus> _logger;

    public KafkaEventBus(
        IOptions<EventBusOptions> options,
        ILogger<KafkaEventBus> logger)
    {
        _options = options.Value;
        _logger = logger;

        // ★ Kafka 生产者配置
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,

            // ★ 学习要点: acks=all + EnableIdempotence 实现最强持久化
            //   - Acks = Acks.All: 等待所有 ISR 副本确认 (即使 Leader 宕机也不丢)
            //   - EnableIdempotence = true: 幂等生产者,防止重试导致重复
            Acks = ParseAcks(_options.ProducerAcks),
            EnableIdempotence = true,

            // ★ 消息发送超时 (超过此时间未确认则失败)
            MessageTimeoutMs = _options.MessageTimeoutMs,

            // ★ 学习要点: 重试配置说明
            //   Confluent.Kafka 2.x 配合 EnableIdempotence=true 时,生产者默认无限重试
            //   (MessageSendMaxRetries 默认值 = int.MaxValue)
            //   无需显式配置 RetryCount,幂等生产者会自动去重
            //   如需限制重试次数,设置 MessageSendMaxRetries 属性

            // ★ 学习要点: 生产者压缩 (减少网络带宽,增加 CPU 开销)
            //   - CompressionType.Lz4: 压缩比与 CPU 平衡,推荐
            //   - Snappy: 旧版本兼容,新项目用 Lz4
            CompressionType = CompressionType.Lz4,

            // ★ 学习要点: 序列化器
            //   - Key 用 string (订单号),Value 用 string (JSON)
            //   - 也可以用自定义 Avro/Protobuf 序列化器,但 JSON 学习成本低
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
        _logger.LogInformation("KafkaEventBus 初始化完成, BootstrapServers={Servers}",
            _options.BootstrapServers);
    }

    /// <summary>
    /// 发布集成事件到 Kafka
    /// </summary>
    public async Task PublishAsync<TEvent>(
        string topic,
        TEvent @event,
        CancellationToken ct = default) where TEvent : IIntegrationEvent
    {
        try
        {
            // ★ 学习要点: 消息序列化 (JSON)
            //   - 使用 System.Text.Json,配置 CamelCase 命名 (前端/跨语言友好)
            //   - 集成事件是 record,JSON 序列化友好
            var json = JsonSerializer.Serialize(@event, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            // ★ 学习要点: 消息 Key 设计
            //   - 提取业务键 (订单号) 作为 Key,保证同一订单的事件路由到同一 Partition
            //   - Partition 内消息有序 → 同订单事件 FIFO 消费
            var key = ExtractKey(@event);

            // ★ 构造 Kafka 消息
            var message = new Message<string, string>
            {
                Key = key,
                Value = json,

                // ★ 学习要点: 消息 Headers (元数据,不参与 Key 路由)
                //   - 放 EventType 用于消费者路由分发 (一个 Topic 多种事件类型时)
                //   - 放 TraceId 用于链路追踪 (M4 阶段填充)
                Headers = new Headers
                {
                    { "event-type", System.Text.Encoding.UTF8.GetBytes(@event.EventType) },
                    { "event-id", System.Text.Encoding.UTF8.GetBytes(@event.EventId) }
                },

                // ★ 消息时间戳 (默认自动取当前时间)
                Timestamp = new Timestamp(@event.OccurredAt.UtcDateTime)
            };

            // ★ 发送消息 (异步,等待 acks)
            //   - 学习要点: ProduceAsync vs Produce
            //     ProduceAsync: 等待确认,失败抛异常,适合关键消息
            //     Produce:      不等待确认,通过 DeliveryReport 回调,适合高吞吐
            //   - 支付场景资金关键,用 ProduceAsync
            var deliveryResult = await _producer.ProduceAsync(topic, message, ct);

            _logger.LogInformation(
                "Kafka 发布成功: topic={Topic}, partition={Partition}, offset={Offset}, key={Key}, eventId={EventId}",
                deliveryResult.Topic, deliveryResult.Partition, deliveryResult.Offset, key, @event.EventId);
        }
        catch (ProduceException<string, string> ex)
        {
            // ★ 学习要点: Kafka 错误分类
            //   - ErrorType.Local: 本地错误 (序列化失败/连接失败),可重试
            //   - ErrorType.Broker: Broker 错误 (Topic 不存在/权限不足),需运维介入
            _logger.LogError(ex,
                "Kafka 发布失败: topic={Topic}, error={Error}, isBroker={IsBroker}",
                topic, ex.Error.Reason, ex.Error.IsBrokerError);
            throw;
        }
    }

    /// <summary>从事件实例提取业务 Key (订单号,决定 Partition 路由)</summary>
    private static string ExtractKey<TEvent>(TEvent @event) where TEvent : IIntegrationEvent
    {
        // 学习要点: 用反射提取 OrderNo/OrderId 属性作为 Key
        //   生产场景应在 IIntegrationEvent 接口定义 Key 属性,这里简化用反射
        var type = @event.GetType();
        var orderNoProp = type.GetProperty("OrderNo");
        if (orderNoProp?.GetValue(@event) is string orderNo && !string.IsNullOrEmpty(orderNo))
            return orderNo;

        var orderIdProp = type.GetProperty("OrderId");
        if (orderIdProp?.GetValue(@event) is long orderId && orderId > 0)
            return orderId.ToString();

        // 无业务键时用 EventId,均匀分布到不同 Partition
        return @event.EventId;
    }

    /// <summary>解析 acks 配置字符串</summary>
    private static Acks ParseAcks(string acks)
    {
        return acks?.ToLowerInvariant() switch
        {
            "0" => Acks.None,
            "1" => Acks.Leader,
            "all" or "-1" => Acks.All,
            _ => Acks.All  // 默认 all (最强持久化)
        };
    }

    public void Dispose()
    {
        // ★ 学习要点: Flush 确保所有挂起消息已发送
        //   - 生产者有缓冲区,Dispose 前应 Flush 防止丢消息
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        // ★ 学习要点: 异步 Dispose,先 Flush 再释放
        //   - Flush 10s 超时,超时后强制释放
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        await Task.CompletedTask;
    }
}
