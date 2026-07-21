using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PaymentGateway.Application.Abstractions;

namespace PaymentGateway.Infrastructure.EventBus;

// ============================================================================
// KafkaConsumerService —— Kafka 消费者基类 (BackgroundService 托管)
// ============================================================================
// ★ 学习要点: Kafka 消费者核心概念
//
// 【Consumer Group 与 Rebalance】
//   - 同一 ConsumerGroup 内消息只投递给一个消费者 (负载均衡)
//   - 不同 ConsumerGroup 各自消费全量 (广播)
//   - Rebalance: 消费者加入/退出时重新分配 Partition (期间暂停消费)
//
// 【Offset 提交策略】
//   - 自动提交 (EnableAutoCommit=true): 消费后定时提交,可能重复消费或丢失
//     (重复: 消费成功但未提交时宕机)
//     (丢失: 提交后业务异常,消息不再投递)
//   - 手动提交 (本工程采用): 业务成功后才提交,保证 At-Least-Once
//
// 【死信队列 (DLQ) 设计】
//   - 消费失败 N 次后,消息转入 DLQ 主题,不再阻塞主流程
//   - DLQ 主题消息由人工排查或补偿任务处理
//   - 学习要点: 死信队列与重试队列区别
//     重试队列: 失败后延迟重试 (本工程未实现,生产可用 Redis ZSet 做延迟队列)
//     死信队列: 不再重试,人工介入
//
// 【At-Least-Once 与消费者幂等】
//   - Kafka 默认 At-Least-Once 语义 (消息至少消费一次,可能重复)
//   - 消费者必须幂等: 通过业务唯一键 (如 biz_no) 去重
//   - 本工程: 入账消费者用 biz_no 唯一约束防重复记账
//
// 【BackgroundService 托管】
//   - 继承 IHostedService,随应用启动自动启动消费循环
//   - ExecuteAsync 内部 while 循环持续消费
//   - 应用停止时 StopAsync 触发 CancellationToken,优雅退出
// ============================================================================

public abstract class KafkaConsumerService<TEvent> : BackgroundService
    where TEvent : class, IIntegrationEvent
{
    private readonly EventBusOptions _options;
    private readonly ILogger _logger;
    private IConsumer<string, string>? _consumer;

    protected abstract string Topic { get; }
    protected virtual string ConsumerGroup => _options.DefaultConsumerGroup;
    protected virtual int MaxRetryCount => _options.MaxRetryCount;

    protected KafkaConsumerService(
        IOptions<EventBusOptions> options,
        ILogger logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>子类实现具体消费逻辑 (返回 true=成功,false=失败)</summary>
    protected abstract Task<bool> HandleAsync(TEvent @event, CancellationToken ct);

    /// <summary>消息消费失败超过重试次数时调用 (转发到死信队列)</summary>
    protected virtual async Task SendToDeadLetterAsync(TEvent @event, Exception ex)
    {
        // ★ 学习要点: 死信队列转发
        //   - 失败消息 + 异常信息序列化为 JSON
        //   - 发送到 DLQ 主题供人工排查
        //   - 本工程简化: 仅日志记录,生产应真正发送到 Kafka DLQ Topic
        _logger.LogError(ex,
            "消息进入死信队列: topic={Topic}, eventId={EventId}, retryCount={Retry}",
            Topic, @event.EventId, MaxRetryCount);
        await Task.CompletedTask;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // ★ 学习要点: BackgroundService.ExecuteAsync 开头必须先 Yield
        //   原因: StartAsync 里 _executeTask = ExecuteAsync(...) 会同步执行到第一个 await
        //   若 ExecuteAsync 在第一个 await 之前进入同步死循环(如 Consume 立即抛异常),
        //   StartAsync 会被阻塞 → host 启动卡住 → Kestrel 永不监听端口
        //   Task.Yield() 让出线程,保证 StartAsync 立即返回,ExecuteAsync 真正异步运行
        await Task.Yield();

        // ★ Kafka 消费者配置
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = ConsumerGroup,

            // ★ 学习要点: 禁用自动提交,改手动提交 (At-Least-Once 保证)
            EnableAutoCommit = false,
            AutoCommitIntervalMs = _options.AutoCommitIntervalMs,

            // ★ 学习要点: 消费位置策略
            //   - Earliest: 从最早未消费位置开始 (首次加入组时)
            //   - Latest: 只消费加入组后的新消息 (默认)
            //   - 本工程学习用 Earliest,便于重新启动后看到全部消息
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // ★ 会话超时 (超时未心跳则触发 rebalance,默认 10s 学习场景调大)
            SessionTimeoutMs = _options.SessionTimeoutMs,

            // ★ 学习要点: Partition 分配策略
            //   - RangeAssignor: 默认,按范围连续分配 (可能导致不均衡)
            //   - RoundRobinAssignor: 轮询分配 (更均衡,推荐)
            //   - CooperativeStickyAssignor: 增量 Rebalance (Kafka 2.4+,最优)
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,

            // ★ 学习要点: 最大拉取消息数 (批处理,默认 500)
            //   - 太小: 频繁拉取浪费网络
            //   - 太大: 单批处理时间长,可能超时触发 rebalance
            MaxPollIntervalMs = 300_000  // 5 分钟,业务处理慢时调大
        };

        _consumer = new ConsumerBuilder<string, string>(config)
            // ★ 学习要点: 消费前 rebalance 监听
            //   - Rebalance 发生时,正在处理的消息可能未提交 offset
            //   - 应在 PartitionsRevoked 时提交已处理 offset
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                _logger.LogWarning("Kafka Rebalance: PartitionsRevoked, partitions={Parts}",
                    string.Join(",", partitions));
            })
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                _logger.LogInformation("Kafka Rebalance: PartitionsAssigned, partitions={Parts}",
                    string.Join(",", partitions));
            })
            .Build();

        // ★ 订阅 Topic
        _consumer.Subscribe(Topic);
        _logger.LogInformation("Kafka 消费者启动: topic={Topic}, group={Group}", Topic, ConsumerGroup);

        try
        {
            // ★ 主消费循环 (stoppingToken 触发时优雅退出)
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // ★ 阻塞拉取消息 (超时 1s,避免无消息时 CPU 100%)
                    //   - 学习要点: Consume 是阻塞调用,通过 CancellationToken 退出
                    //   - 超时设置: 短超时 (1s) 便于及时响应停止信号
                    var consumeResult = _consumer.Consume(stoppingToken);

                    // ★ 反序列化消息 (JSON → TEvent)
                    var @event = JsonSerializer.Deserialize<TEvent>(
                        consumeResult.Message.Value,
                        new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

                    if (@event == null)
                    {
                        _logger.LogWarning("反序列化为 null: topic={Topic}, offset={Offset}",
                            consumeResult.Topic, consumeResult.Offset);
                        continue;
                    }

                    // ★ 调用子类 HandleAsync 处理业务
                    //   - 学习要点: 重试机制 — 失败时指数退避重试 MaxRetryCount 次
                    //   - 超过重试次数 → 进入死信队列
                    var handled = await RetryWithBackoffAsync(@event, stoppingToken);

                    if (handled)
                    {
                        // ★ 处理成功 → 手动提交 Offset
                        //   - 学习要点: 手动提交方式
                        //     Commit(consumeResult): 同步提交 (阻塞,简单但慢)
                        //     StoreOffset + 异步 commit: 异步提交,性能高但可能重复消费
                        //   - 本工程用同步提交,资金场景宁可慢不可丢
                        _consumer.Commit(consumeResult);
                        _logger.LogInformation(
                            "Kafka 消费成功: topic={Topic}, partition={P}, offset={O}, eventId={Eid}",
                            consumeResult.Topic, consumeResult.Partition, consumeResult.Offset, @event.EventId);
                    }
                    else
                    {
                        // 处理失败超过重试次数 → 转死信队列后提交 Offset
                        //   ★ 学习要点: 死信后必须提交 Offset,否则会无限重投
                        await SendToDeadLetterAsync(@event, new InvalidOperationException("消费失败,已达最大重试次数"));
                        _consumer.Commit(consumeResult);
                    }
                }
                catch (ConsumeException ex)
                {
                    // ★ Kafka 消费异常 (非业务异常,如 topic 不存在、broker 不可达)
                    //   学习要点: Consume 在 topic 不存在时会立即抛异常,若 catch 后立即重试
                    //   会变成紧密循环(CPU 100%),故需退避延迟再重试
                    _logger.LogError(ex, "Kafka Consume 异常: {Reason}", ex.Error.Reason);
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
                catch (OperationCanceledException)
                {
                    // 正常停止,退出循环
                    break;
                }
                catch (Exception ex)
                {
                    // 业务异常未被子类处理 → 重试或死信
                    _logger.LogError(ex, "Kafka 消费未知异常");
                }
            }
        }
        finally
        {
            // ★ 学习要点: 优雅退出
            //   - Close 通知 Broker 离开 ConsumerGroup (触发 rebalance)
            //   - 不调用 Close 直接 Dispose 会等到 session timeout 才 rebalance
            _consumer?.Close();
            _consumer?.Dispose();
            _logger.LogInformation("Kafka 消费者已停止: topic={Topic}", Topic);
        }
    }

    /// <summary>指数退避重试</summary>
    private async Task<bool> RetryWithBackoffAsync(TEvent @event, CancellationToken ct)
    {
        for (var attempt = 1; attempt <= MaxRetryCount; attempt++)
        {
            try
            {
                var success = await HandleAsync(@event, ct);
                if (success)
                    return true;

                _logger.LogWarning("消费失败重试 {Attempt}/{Max}: eventId={Eid}",
                    attempt, MaxRetryCount, @event.EventId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "消费异常重试 {Attempt}/{Max}: eventId={Eid}",
                    attempt, MaxRetryCount, @event.EventId);
            }

            // ★ 学习要点: 指数退避 (Exponential Backoff)
            //   - 公式: 2^(attempt-1) * baseDelay (100ms, 200ms, 400ms, 800ms...)
            //   - 避免雪崩: 大量失败同时重试压垮系统
            //   - 加随机抖动避免重试同步化
            if (attempt < MaxRetryCount)
            {
                var backoffMs = Math.Min(100 * Math.Pow(2, attempt - 1), 5000);
                var jitterMs = Random.Shared.NextDouble() * 100;
                await Task.Delay((int)(backoffMs + jitterMs), ct);
            }
        }
        return false;
    }
}
