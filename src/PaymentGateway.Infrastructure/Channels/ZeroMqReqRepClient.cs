using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NetMQ;
using NetMQ.Sockets;
using PaymentGateway.Infrastructure.Tracing;

namespace PaymentGateway.Infrastructure.Channels;

/// <summary>
/// ZeroMQ REQ/REP 同步通信客户端 —— 风控/反欺诈子服务调用场景
/// 学习要点:
///   1. ZeroMQ 四种核心 socket 模式:
///        REQ/REP: 严格请求-应答(客户端发,服务端收,服务端回,客户端收,严格交替)
///        DEALER/ROUTER: 异步版 REQ/REP(支持多请求并发)
///        PUB/SUB: 广播订阅(发布者不关心是否有订阅者)
///        PUSH/PULL: 任务分发(生产者-消费者管道)
///   2. REQ/REP 的"严格交替"约束:
///        - REQ 发送后必须先收一次才能再发(状态机驱动)
///        - REP 接收后必须先发一次才能再收
///        违反约束会抛 FrameReceivedWithoutSend 等异常
///   3. 学习工程示例场景: API 进程作为 REQ 客户端,调用风控子服务的 REP 服务端
///      实际生产中若风控服务需要水平扩展,应改用 DEALER/ROUTER 模式
///   4. NetMQ 线程模型: 内部维护 Poller 线程,socket 非线程安全
///        本封装通过 lock 保护 Send/Receive,避免并发调用破坏状态机
///   5. 注意: NetMQ 默认会创建自己的 Context (NetMQContext),
///        调用 NetMQConfig.Cleanup() 在应用退出时清理(见 DependencyInjection 注册)
/// </summary>
public sealed class ZeroMqReqRepClient : IDisposable
{
    private readonly RequestSocket _socket;
    private readonly ZeroMqOptions _options;
    private readonly ILogger<ZeroMqReqRepClient> _logger;
    private readonly object _lock = new();
    private bool _disposed;

    public ZeroMqReqRepClient(
        IOptions<ZeroMqOptions> options,
        ILogger<ZeroMqReqRepClient> logger)
    {
        _options = options.Value;
        _logger = logger;

        // 创建 REQ socket 并连接到服务端
        // 学习要点: REQ 端用 Connect,服务端 REP 用 Bind
        //   单例持有 socket,避免每次请求创建(ZeroMQ 连接建立有成本)
        _socket = new RequestSocket();
        _socket.Connect(_options.ReqRepEndpoint);

        // 设置接收超时,避免请求卡死
        // 学习要点: ReceiveTimeout 是 NetMQ 提供的 Receive 阻塞超时
        _socket.Options.ReceiveHighWatermark = 1000;
    }

    /// <summary>
    /// 发送请求并等待应答(同步阻塞)
    /// </summary>
    /// <param name="request">请求内容(字符串)</param>
    /// <returns>应答内容</returns>
    /// <exception cref="TimeoutException">等待应答超时</exception>
    public string Request(string request)
    {
        // 学习要点: lock 保护 socket,因 REQ socket 状态机非线程安全
        //   并发 Send 会破坏状态机,导致后续 Receive 抛异常
        lock (_lock)
        {
            // ★ 业务 Span 埋点 —— 在 Jaeger 中可看到 zeroMQ.request Span
            using var span = TraceContext.StartSpan(
                "zeroMQ.request",
                ("messaging.system", "zeromq"),
                ("messaging.destination", _options.ReqRepEndpoint),
                ("request.size", request.Length));

            try
            {
                _logger.LogDebug("ZeroMQ REQ -> {Endpoint}: {Request}",
                    _options.ReqRepEndpoint, request);

                // 发送请求帧(NetMQ 用 SendFrame)
                _socket.SendFrame(request);

                // 等待应答,带超时
                // 学习要点: TryReceiveFrame 不会阻塞,返回 false 表示超时
                if (!_socket.TryReceiveFrameString(
                        TimeSpan.FromMilliseconds(_options.RequestTimeoutMs),
                        out var response))
                {
                    span?.SetTag("error", true);
                    TraceContext.RecordError(
                        new TimeoutException($"ZeroMQ 等待应答超时 ({_options.RequestTimeoutMs}ms)"));
                    throw new TimeoutException(
                        $"ZeroMQ 请求超时,endpoint={_options.ReqRepEndpoint}");
                }

                _logger.LogDebug("ZeroMQ REP <- {Endpoint}: {Response}",
                    _options.ReqRepEndpoint, response);
                span?.SetTag("response.size", response?.Length ?? 0);

                return response ?? string.Empty;
            }
            catch (Exception ex) when (ex is not TimeoutException)
            {
                TraceContext.RecordError(ex);
                _logger.LogError(ex, "ZeroMQ 请求失败,endpoint={Endpoint}",
                    _options.ReqRepEndpoint);
                throw;
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _socket.Dispose();
        _disposed = true;
    }
}
