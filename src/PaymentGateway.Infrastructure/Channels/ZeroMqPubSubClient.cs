using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NetMQ;
using NetMQ.Sockets;
using PaymentGateway.Infrastructure.Tracing;

namespace PaymentGateway.Infrastructure.Channels;

/// <summary>
/// ZeroMQ PUB/SUB 异步广播客户端 —— 状态变更/事件广播场景
/// 学习要点:
///   1. PUB/SUB 模式特点:
///        - 发布者(PublisherSocket)不关心是否有订阅者,只管广播
///        - 订阅者(SubscriberSocket)可订阅特定 Topic(前缀过滤)
///        - 消息不会持久化,订阅者掉线期间的消息全部丢失
///        - 默认"join-late"行为: 订阅者只能收到订阅之后的消息
///   2. 支付网关典型场景:
///        - PUB/SUB: 账户余额变更后广播给所有关心此事件的子系统(风控/反洗钱/分析)
///        - 跨进程解耦,PUB 不阻塞业务流程
///   3. Topic 过滤:
///        - SUB 端 Subscribe(topic) 订阅前缀匹配的消息
///        - 例如 Subscribe("account.") 会收到 "account.credit" / "account.debit"
///   4. 与 Kafka 的差异:
///        - Kafka 有消费组、Offset、At-Least-Once 保证
///        - ZeroMQ PUB/SUB 无任何可靠性保证,适合"尽力通知"场景
///        - 重要业务事件应用 Kafka,状态变化通知可用 ZeroMQ
///   5. 性能: inproc 通信每秒可达百万级消息,远超 Kafka
/// </summary>
public sealed class ZeroMqPubSubClient : IDisposable
{
    private readonly PublisherSocket _socket;
    private readonly ZeroMqOptions _options;
    private readonly ILogger<ZeroMqPubSubClient> _logger;
    private bool _disposed;

    public ZeroMqPubSubClient(
        IOptions<ZeroMqOptions> options,
        ILogger<ZeroMqPubSubClient> logger)
    {
        _options = options.Value;
        _logger = logger;

        // 创建 PUB socket 并绑定端口
        // 学习要点: PUB 端用 Bind,作为消息源服务端
        //   若本工程作为客户端发布到外部服务,改用 Connect
        _socket = new PublisherSocket();
        _socket.Bind(_options.PubSubEndpoint);

        // 高水位标记: 缓冲区最大消息数,超过后丢弃旧消息
        // 学习要点: SendHighWatermark 控制内存占用,避免慢订阅者拖垮发布者
        _socket.Options.SendHighWatermark = 10000;

        _logger.LogInformation("ZeroMQ PUB 已绑定 {Endpoint}", _options.PubSubEndpoint);
    }

    /// <summary>
    /// 发布消息(带 Topic 标签)
    /// </summary>
    /// <param name="topic">主题(如 "account.credit"),SUB 端按前缀过滤</param>
    /// <param name="payload">消息内容</param>
    public void Publish(string topic, string payload)
    {
        // ★ 业务 Span 埋点
        using var span = TraceContext.StartSpan(
            "zeroMQ.publish",
            ("messaging.system", "zeromq"),
            ("messaging.destination", _options.PubSubEndpoint),
            ("messaging.topic", topic),
            ("payload.size", payload.Length));

        try
        {
            // 学习要点: NetMQ 多帧消息
            //   第一帧是 Topic(必须先发),第二帧是 Payload
            //   Subscribe(topic) 时按第一帧前缀匹配
            _socket.SendMoreFrame(topic).SendFrame(payload);

            _logger.LogDebug("ZeroMQ PUB topic={Topic} payload={Payload}",
                topic, payload);
        }
        catch (Exception ex)
        {
            TraceContext.RecordError(ex);
            _logger.LogError(ex, "ZeroMQ 发布失败,topic={Topic}", topic);
            throw;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _socket.Dispose();
        _disposed = true;
    }
}

/// <summary>
/// ZeroMQ SUB 订阅者辅助封装 —— 用于业务订阅者
/// 学习要点:
///   1. SubscriberSocket 必须显式 Subscribe(topic) 才会收到消息
///   2. ReceiveReady 事件驱动模式,配合 NetMQPoller 异步轮询
///   3. 本封装提供阻塞 Receive 方法,实际生产中应改用 Poller 事件回调
/// </summary>
public sealed class ZeroMqSubscriber : IDisposable
{
    private readonly SubscriberSocket _socket;
    private readonly ILogger<ZeroMqSubscriber> _logger;
    private bool _disposed;

    public ZeroMqSubscriber(string endpoint, string topicPrefix, ILogger<ZeroMqSubscriber> logger)
    {
        _logger = logger;
        _socket = new SubscriberSocket();
        _socket.Connect(endpoint);

        // 学习要点: Subscribe("") 订阅所有;Subscribe("account.") 订阅前缀匹配
        _socket.Subscribe(topicPrefix);
        _socket.Options.ReceiveHighWatermark = 10000;

        _logger.LogInformation("ZeroMQ SUB 已订阅 {Endpoint} topic={Topic}",
            endpoint, topicPrefix);
    }

    /// <summary>
    /// 同步接收一条消息(阻塞,带超时)
    /// </summary>
    /// <param name="timeoutMs">超时(毫秒)</param>
    /// <returns>(Topic, Payload);超时返回 null</returns>
    public (string Topic, string Payload)? Receive(int timeoutMs = 5000)
    {
        if (!_socket.TryReceiveFrameString(TimeSpan.FromMilliseconds(timeoutMs), out var topic))
        {
            return null;
        }

        // 第二帧是 Payload
        if (!_socket.TryReceiveFrameString(TimeSpan.FromMilliseconds(timeoutMs), out var payload))
        {
            return null;
        }

        return (topic ?? string.Empty, payload ?? string.Empty);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _socket.Dispose();
        _disposed = true;
    }
}
