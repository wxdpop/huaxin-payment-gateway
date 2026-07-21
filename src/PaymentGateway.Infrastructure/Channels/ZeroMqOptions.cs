namespace PaymentGateway.Infrastructure.Channels;

/// <summary>
/// ZeroMQ 通道配置 —— 通过 appsettings.json "ZeroMq" 节绑定
/// 学习要点:
///   1. ZeroMQ 是异步消息库,无需独立 Broker(进程内通信极快)
///      与 Kafka 区别:
///        - Kafka: 持久化、跨节点、At-Least-Once、吞吐量高
///        - ZeroMQ: 进程间/线程间、无持久化、低延迟、可堆叠拓扑
///      支付网关典型用途:
///        - REQ/REP: 风控子服务调用(同步返回风险等级)
///        - PUB/SUB: 跨进程广播账户状态变更(异步订阅)
///   2. ZeroMQ endpoint 格式:
///        - tcp://host:port       跨节点 TCP
///        - ipc:///tmp/xxx.sock   Unix Domain Socket
///        - inproc://xxx          进程内(线程间),无网络开销
///   3. 学习工程中两个 endpoint 分别对应 REQ/REP 服务端(本工程作为 REP) 与 PUB 服务端
/// </summary>
public class ZeroMqOptions
{
    /// <summary>
    /// REQ/REP 通信的本地绑定地址(本工程作为 REP 服务端)
    /// 学习要点: bind 表示作为服务端监听,connect 表示作为客户端连接
    ///   单体+模块化架构中,API 进程内通信用 inproc,跨进程用 tcp
    /// </summary>
    public string ReqRepEndpoint { get; set; } = "inproc://risk-check";

    /// <summary>
    /// PUB/SUB 通信的发布端绑定地址
    /// </summary>
    public string PubSubEndpoint { get; set; } = "tcp://*:5556";

    /// <summary>
    /// 请求超时(毫秒),REQ/REP 同步等待最大时长
    /// 学习要点: 超时后抛 TimeoutException,避免线程被卡住
    /// </summary>
    public int RequestTimeoutMs { get; set; } = 3000;

    /// <summary>
    /// 是否启用 ZeroMQ(本地开发可关闭以减少依赖)
    /// </summary>
    public bool Enabled { get; set; } = true;
}
