// ============================================================================
// Kafka C# 客户端抽象 + 内存模拟实现
// ============================================================================
// 学习要点:
//   1. 用接口抽象 Kafka 客户端,生产可换 Confluent.Kafka
//   2. InMemoryKafka 模拟 Kafka 核心机制: Topic/Partition/Offset/ConsumerGroup
//   3. 不启动 Kafka 服务也能学习 API
//
// 真实环境替换:
//   1. NuGet: dotnet add package Confluent.Kafka
//   2. Docker: docker run -d --name kafka -p 9092:9092 \
//                -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
//                confluentinc/cp-kafka
//   3. 把 InMemoryKafka 换成 Confluent.Kafka.IProducer/IConsumer
// ============================================================================

namespace KafkaDemo;

// ============================================================================
// 消息实体
// ============================================================================

public record KafkaMessage(
    string Topic,
    int Partition,
    long Offset,
    string? Key,
    string Value,
    DateTime Timestamp);

// ============================================================================
// 生产者接口
// ============================================================================

public interface IKafkaProducer : IDisposable
{
    Task<ProduceResult> ProduceAsync(string topic, string? key, string value);
}

public record ProduceResult(int Partition, long Offset);

// ============================================================================
// 消费者接口
// ============================================================================

public interface IKafkaConsumer : IDisposable
{
    void Subscribe(string topic);
    KafkaMessage? Consume(TimeSpan timeout);
    void Commit(KafkaMessage message);
}

// ============================================================================
// 内存版 Kafka 模拟实现
// ============================================================================

/// <summary>
/// 内存版 Kafka(单机模拟,学习用)
/// 真实 Kafka 是分布式 Broker 集群,这里简化为单进程
/// </summary>
public class InMemoryKafka
{
    // Topic 配置
    private readonly Dictionary<string, int> _topicPartitions = new();
    // 每个分区的消息队列(按 Offset 排序)
    private readonly Dictionary<string, List<List<KafkaMessage>>> _partitions = new();
    // 每个 ConsumerGroup 在每个分区的消费进度
    private readonly Dictionary<string, Dictionary<(string, int), long>> _groupOffsets = new();
    // 锁
    private readonly object _lock = new();

    /// <summary>
    /// 创建 Topic
    /// </summary>
    public void CreateTopic(string topic, int partitions = 3)
    {
        lock (_lock)
        {
            if (_topicPartitions.ContainsKey(topic)) return;
            _topicPartitions[topic] = partitions;
            _partitions[topic] = new List<List<KafkaMessage>>();
            for (int i = 0; i < partitions; i++)
                _partitions[topic].Add(new List<KafkaMessage>());
            Console.WriteLine($"[Kafka] Topic 已创建: {topic} ({partitions} partitions)");
        }
    }

    /// <summary>
    /// 生产消息(Key 决定分区)
    /// </summary>
    public ProduceResult Produce(string topic, string? key, string value)
    {
        lock (_lock)
        {
            if (!_topicPartitions.TryGetValue(topic, out var partitionCount))
                throw new InvalidOperationException($"Topic 不存在: {topic}");

            // 计算 Key 对应的分区
            int partition;
            if (key == null)
                partition = Random.Shared.Next(partitionCount);
            else
                partition = Math.Abs(key.GetHashCode()) % partitionCount;

            var msgList = _partitions[topic][partition];
            var msg = new KafkaMessage(
                Topic: topic,
                Partition: partition,
                Offset: msgList.Count,
                Key: key,
                Value: value,
                Timestamp: DateTime.UtcNow);
            msgList.Add(msg);

            return new ProduceResult(partition, msg.Offset);
        }
    }

    /// <summary>
    /// 消费消息(按 ConsumerGroup 维护 Offset)
    /// </summary>
    public KafkaMessage? Consume(string group, string topic, TimeSpan timeout)
    {
        lock (_lock)
        {
            if (!_topicPartitions.TryGetValue(topic, out var partitionCount))
                return null;

            // 初始化 Group Offset
            if (!_groupOffsets.ContainsKey(group))
            {
                _groupOffsets[group] = new Dictionary<(string, int), long>();
                for (int i = 0; i < partitionCount; i++)
                    _groupOffsets[group][(topic, i)] = 0;
            }

            // 轮询每个分区
            for (int i = 0; i < partitionCount; i++)
            {
                var offset = _groupOffsets[group][(topic, i)];
                var msgList = _partitions[topic][i];
                if (offset < msgList.Count)
                {
                    var msg = msgList[(int)offset];
                    return msg;   // 不自动提交,需调用 Commit
                }
            }
            return null;
        }
    }

    /// <summary>
    /// 提交消费位移
    /// </summary>
    public void Commit(string group, KafkaMessage msg)
    {
        lock (_lock)
        {
            if (!_groupOffsets.ContainsKey(group)) return;
            _groupOffsets[group][(msg.Topic, msg.Partition)] = msg.Offset + 1;
        }
    }

    // 简化版生产者/消费者包装
    public InMemoryProducer CreateProducer() => new(this);
    public InMemoryConsumer CreateConsumer(string group) => new(this, group);
}

public class InMemoryProducer : IKafkaProducer
{
    private readonly InMemoryKafka _kafka;
    public InMemoryProducer(InMemoryKafka kafka) { _kafka = kafka; }
    public Task<ProduceResult> ProduceAsync(string topic, string? key, string value)
    {
        var result = _kafka.Produce(topic, key, value);
        return Task.FromResult(result);
    }
    public void Dispose() { }
}

public class InMemoryConsumer : IKafkaConsumer
{
    private readonly InMemoryKafka _kafka;
    private readonly string _group;
    private string? _topic;

    public InMemoryConsumer(InMemoryKafka kafka, string group)
    {
        _kafka = kafka;
        _group = group;
    }

    public void Subscribe(string topic) => _topic = topic;

    public KafkaMessage? Consume(TimeSpan timeout) =>
        _topic != null ? _kafka.Consume(_group, _topic, timeout) : null;

    public void Commit(KafkaMessage message) => _kafka.Commit(_group, message);
    public void Dispose() { }
}
