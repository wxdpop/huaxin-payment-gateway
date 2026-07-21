using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;

namespace PaymentGateway.Infrastructure.EventBus;

// ============================================================================
// InMemoryEventBus —— 进程内事件总线 (无 Kafka 依赖的轻量备选)
// ============================================================================
// ★ 学习要点: 这是 IEventBus 的轻量实现,用于无 Kafka 环境的本地开发与测试
//
// 【使用场景】
//   1. 本地开发未启动 Kafka,但想跑通业务流程
//   2. 单元测试不想依赖 Kafka,简化测试环境
//   3. 单体应用内部的事件传播 (无跨服务需求)
//
// 【实现原理】
//   - 用 ConcurrentDictionary<string, List<Func<IIntegrationEvent, Task>>> 注册订阅者
//   - Publish 时遍历所有订阅者调用 (同步执行,不跨进程)
//
// 【与 KafkaEventBus 的差异】
//   - KafkaEventBus: 跨进程,持久化,At-Least-Once,生产可用
//   - InMemoryEventBus: 进程内,无持久化,程序崩溃则丢失,仅用于开发测试
//
// 【Channel 通道模式】
//   - 本实现简化为同步调用,生产场景可用 System.Threading.Channels 做异步队列
//   - Channel 是 .NET 高性能生产者-消费者队列,适合做进程内事件总线
// ============================================================================

public class InMemoryEventBus : IEventBus
{
    // ★ 学习要点: ConcurrentDictionary 线程安全事件订阅表
    //   - Key: Topic 名称
    //   - Value: 该 Topic 下的所有订阅者 (List)
    //   - 注意: List 不是线程安全的,用 ConcurrentBag 或 lock 包裹
    private readonly ConcurrentDictionary<string, List<Func<IIntegrationEvent, Task>>> _subscribers
        = new();
    private readonly ILogger<InMemoryEventBus> _logger;

    public InMemoryEventBus(ILogger<InMemoryEventBus> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 发布事件到进程内事件总线
    /// </summary>
    public async Task PublishAsync<TEvent>(
        string topic,
        TEvent @event,
        CancellationToken ct = default) where TEvent : IIntegrationEvent
    {
        if (!_subscribers.TryGetValue(topic, out var subscribers) || subscribers.Count == 0)
        {
            _logger.LogDebug("InMemoryEventBus: 无订阅者,丢弃: topic={Topic}, eventId={Eid}",
                topic, @event.EventId);
            return;
        }

        // ★ 学习要点: 遍历订阅者调用 (复制一份避免遍历中修改)
        var handlers = subscribers.ToList();
        _logger.LogInformation("InMemoryEventBus 发布: topic={Topic}, eventId={Eid}, 订阅者数={Count}",
            topic, @event.EventId, handlers.Count);

        foreach (var handler in handlers)
        {
            try
            {
                await handler(@event);
            }
            catch (Exception ex)
            {
                // 单个订阅者异常不影响其他订阅者
                _logger.LogError(ex, "InMemoryEventBus 订阅者异常: topic={Topic}", topic);
            }
        }
    }

    /// <summary>
    /// 订阅主题 (非接口方式,简化版)
    /// </summary>
    /// <remarks>
    /// ★ 学习要点: 进程内事件总线的简化订阅 API
    ///   与 KafkaEventBus 不同,这里用回调函数而非接口,降低学习成本
    ///   生产场景应改为 IIntegrationEventConsumer<TEvent> 接口分发
    /// </remarks>
    public void Subscribe<TEvent>(string topic, Func<TEvent, Task> handler)
        where TEvent : IIntegrationEvent
    {
        // ★ 学习要点: ConcurrentDictionary.GetOrAdd + List 加锁
        //   - GetOrAdd 保证 List 存在
        //   - List.Add 不是线程安全,需 lock
        //   - 改用 ConcurrentBag 可避免 lock,但不保留顺序
        var list = _subscribers.GetOrAdd(topic, _ => new List<Func<IIntegrationEvent, Task>>());
        lock (list)
        {
            list.Add(async e => await handler((TEvent)e));
        }
        _logger.LogInformation("InMemoryEventBus 订阅: topic={Topic}", topic);
    }
}
