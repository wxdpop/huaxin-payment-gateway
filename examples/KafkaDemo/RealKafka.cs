// ============================================================================
// 真实 Kafka 客户端适配器(基于 Confluent.Kafka 2.x)
// ============================================================================
// 学习要点:
//   1. Confluent.Kafka 是基于 librdkafka(C 库)的官方 .NET 客户端
//      - 命名空间 Confluent.Kafka
//      - 配置类 ProducerConfig / ConsumerConfig(字典风格)
//      - Producer/Consumer 都是泛型 <K, V>,K/V 需实现 ISerializer
//      - 内置 string/byte[]/int/long 等 ISerializer
//   2. Producer 配置要点:
//      - BootstrapServers: Broker 地址列表(逗号分隔)
//      - Acks: 0/1/All (0=不等 ACK,1=Leader ACK,All=ISR 全 ACK)
//      - EnableIdempotence: true 开启幂等生产(防重复)
//   3. Consumer 配置要点:
//      - GroupId: Consumer Group 标识(同 Group 内分区负载均衡)
//      - AutoOffsetReset: Earliest/Latest/Error (无提交位移时的策略)
//      - EnableAutoCommit: false (本 Demo 手动提交,精确控制)
//
// 使用前提:
//   1. Docker 启动 Kafka(多 listener 模式 - 关键):
//      ★踩坑: 必须配置 EXTERNAL listener 广播 host 可解析的地址
//      - LISTENERS: Kafka 监听哪些地址
//      - ADVERTISED_LISTENERS: Broker 告诉客户端连哪个地址(元数据响应中返回)
//      - 内部用 INTERNAL listener(kafka:9092,Docker 内部互通)
//      - 外部用 EXTERNAL listener(localhost:29092,host 机器访问)
//
//      docker run -d --name kafka \
//        -p 9092:9092 -p 29092:29092 \
//        -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
//        -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092 \
//        -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:9092,EXTERNAL://localhost:29092 \
//        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
//        -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
//        confluentinc/cp-kafka
//
//   2. NuGet: dotnet add package Confluent.Kafka
//
// 启动:
//   dotnet run --project examples/KafkaDemo -- real
//   dotnet run --project examples/KafkaDemo -- real localhost:29092
// ============================================================================

using Confluent.Kafka;
using KafkaDemo;

namespace KafkaDemo;

/// <summary>
/// 真实 Kafka 生产者(适配 Confluent.Kafka.IProducer 到 IKafkaProducer)
/// </summary>
public class RealKafkaProducer : IKafkaProducer
{
    private readonly IProducer<string, string> _producer;

    public RealKafkaProducer(string bootstrapServers = "localhost:29092")
    {
        // 学习要点: Producer 核心配置
        //   - Acks.All: ISR 副本都确认才算写入成功(最高一致性,牺牲性能)
        //   - EnableIdempotence: 开启幂等生产,自动去重(基于 PID + SeqNum)
        //     ★注意: 开启幂等后 MaxInFlight 必须 <= 5,本 Demo 单线程无影响
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            // 学习要点: 消息发送超时(网络分区时避免无限重试)
            MessageTimeoutMs = 10000,
            // 学习要点: 重试间隔(避免重试风暴)
            RetryBackoffMs = 100,
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
        Console.WriteLine($"[Kafka Producer] 已连接 {bootstrapServers}");
    }

    public async Task<ProduceResult> ProduceAsync(string topic, string? key, string value)
    {
        // 学习要点: Kafka 消息的 Key 决定分区
        //   - Key=null: 轮询分区(无顺序保证)
        //   - Key 相同: 一定进入同一分区(分区内严格有序)
        //   - 内部用 murmur2 算法 hash(Key) % partitionCount
        var msg = new Message<string, string>
        {
            Key = key!,
            Value = value,
            // Timestamp 默认由 Broker 设置(LogAppendTime),也可显式指定 CreateTime
            Timestamp = new Timestamp(DateTimeOffset.UtcNow)
        };

        // ProduceAsync 返回 DeliveryResult<K,V>,含 Partition/Offset/Status
        var dr = await _producer.ProduceAsync(topic, msg);

        if (dr.Status != PersistenceStatus.Persisted)
            throw new InvalidOperationException($"消息写入失败: {dr.Status}");

        return new ProduceResult(dr.Partition.Value, dr.Offset.Value);
    }

    public void Dispose()
    {
        // 学习要点: Producer 关闭前必须 Flush,确保内部缓冲区消息全部发送
        //   - 不 Flush 直接 Dispose 会丢失未发送的消息
        //   - Flush(TimeSpan) 设置超时避免无限等待
        try { _producer.Flush(TimeSpan.FromSeconds(10)); }
        catch { /* 忽略 */ }
        _producer.Dispose();
    }
}

/// <summary>
/// 真实 Kafka 消费者(适配 Confluent.Kafka.IConsumer 到 IKafkaConsumer)
/// </summary>
public class RealKafkaConsumer : IKafkaConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private bool _subscribed;

    public RealKafkaConsumer(string bootstrapServers, string groupId)
    {
        // 学习要点: Consumer 核心配置
        //   - GroupId: 同 Group 内分区负载均衡(每个分区只分给组内一个 Consumer)
        //              不同 Group 独立消费(广播模式)
        //   - AutoOffsetReset.Earliest: 无提交位移时从最早消息开始消费
        //     Latest: 只消费订阅后的新消息(默认)
        //     Error: 抛异常
        //   - EnableAutoCommit=false: 关闭自动提交,改用手动 Commit 精确控制
        //     自动提交每 5s 提交一次,可能导致重复消费或消息丢失
        //   - PartitionAssignmentStrategy: RangeAssignor/RoundRobinAssignor/CooperativeStickyAssignor
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            // 学习要点: SessionTimeoutMs: 心跳超时(超过此时间未心跳,触发 Rebalance)
            SessionTimeoutMs = 10000,
            // 学习要点: MaxPollIntervalMs: 两次 Poll 最大间隔(超时触发 Rebalance)
            //   防止消费者处理太慢被踢出 Group
            MaxPollIntervalMs = 300000,
        };

        _consumer = new ConsumerBuilder<string, string>(config).Build();
        Console.WriteLine($"[Kafka Consumer] 已连接 {bootstrapServers}, GroupId={groupId}");
    }

    public void Subscribe(string topic)
    {
        // 学习要点: Subscribe 是动态订阅,由 Broker 协调分区分配
        //   另一种方式 Assign 是手动指定分区(不走 Rebalance 机制)
        _consumer.Subscribe(topic);
        _subscribed = true;
    }

    public KafkaMessage? Consume(TimeSpan timeout)
    {
        if (!_subscribed)
            throw new InvalidOperationException("请先调用 Subscribe 订阅 Topic");

        // 学习要点: Confluent.Kafka 2.x 的 Consume 是阻塞拉取
        //   - Consume(int ms, CancellationToken): 带超时,无消息返回 null
        //   - Consume(CancellationToken): 无超时,需外部 CancellationToken 控制
        //   返回 ConsumeResult<K,V> 或在超时时返回 null
        var timeoutMs = (int)timeout.TotalMilliseconds;
        ConsumeResult<string, string>? result;

        try
        {
            result = _consumer.Consume(timeoutMs);
        }
        catch (ConsumeException ex)
        {
            Console.WriteLine($"[Kafka 消费异常] {ex.Error.Code}: {ex.Error.Reason}");
            return null;
        }

        if (result == null) return null;
        if (result.IsPartitionEOF) return null;  // 学习要点: 分区消费到末尾

        // 学习要点: ConsumeResult 包含完整的消息元数据
        //   - Topic/Partition/Offset: 消息位置
        //   - Message.Key/Value: 消息内容
        //   - Message.Timestamp.Type: CreateTime/LogAppendTime/NotAvailable
        return new KafkaMessage(
            Topic: result.Topic,
            Partition: result.Partition.Value,
            Offset: result.Offset.Value,
            Key: result.Message.Key,
            Value: result.Message.Value,
            Timestamp: result.Message.Timestamp.UtcDateTime);
    }

    public void Commit(KafkaMessage message)
    {
        // 学习要点: 手动提交 Offset
        //   - Commit(ConsumeResult): 提交整批消息(简化版)
        //   - Commit(IEnumerable<TopicPartitionOffset>): 提交精确位移
        //   ★注意: 提交的是"下次消费的起始 Offset",即当前 Offset + 1
        //          消息 {Offset=5} 处理完后应提交 Offset=6
        var tpo = new TopicPartitionOffset(
            new TopicPartition(message.Topic, new Partition(message.Partition)),
            new Offset(message.Offset + 1));
        _consumer.Commit(new[] { tpo });
    }

    public void Dispose()
    {
        try { _consumer.Close(); }  // Close: 主动离开 Group(触发 Rebalance)
        catch { /* 忽略 */ }
        _consumer.Dispose();
    }
}
