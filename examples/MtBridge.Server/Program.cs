// ============================================================================
// MT 对接 C# 服务端 - ZeroMQ 接入 + Channel 内部管道演示
// ============================================================================
// 架构:
//   EA / Client
//     ↓ ZeroMQ (跨进程,JSON 字节流)
//   [REP Socket] ──> [Channel<Tick>]
//                       ↓
//                   [Dispatcher 消费者]
//                       ├─> [Channel<RiskTask>] ──> 风控消费者(异常价告警)
//                       └─> [Channel<StatTask>] ──> 统计消费者(均价/极值)
//
// 关键点:
//   1. ZeroMQ 负责"对外"通信(跨进程/跨语言)
//   2. Channel 负责"对内"分发(单 Dispatcher → 多下游 Channel = fan-out 广播)
//   3. 有界 Channel + DropOldest = 背压策略(消费跟不上时丢旧 Tick)
//   4. REQ-REP 协议要求 REP 端收到请求必须立即响应,所以用 TryWrite(非阻塞) 入队
//
// Channel 多消费者模式说明:
//   - 同一个 ChannelReader 上调多次 ReadAllAsync = "竞争消费"(每条 Tick 只给一个消费者)
//   - 想让每条 Tick 被多个消费者同时处理(fan-out),必须每个下游独立一个 Channel
//   - 本示例用 Dispatcher 模式实现 fan-out
//
// 协议:
//   请求: {"symbol":"EURUSD","price":1.1050,"time":"...","seq":1}
//   响应: {"ok":true,"msg":"已收到 EURUSD @ 1.10500","server_time":"...","queue":99}
//
// 启动:
//   dotnet run --project examples/MtBridge.Server
// ============================================================================

using NetMQ;
using NetMQ.Sockets;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;

// ============================================================================
// 主程序(顶级语句)
// ============================================================================
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// 2.1 三级管道:
//   - ingestCh: REP 写入,Dispatcher 读取
//   - riskCh:   Dispatcher 写入,风控消费者读取
//   - statCh:   Dispatcher 写入,统计消费者读取
// 每个 Channel 容量 100,满了 DropOldest(背压:消费跟不上丢旧 Tick)
var ingestCh = CreateChannel();
var riskCh   = CreateChannel();
var statCh   = CreateChannel();
static Channel<Tick> CreateChannel() =>
    Channel.CreateBounded<Tick>(new BoundedChannelOptions(100)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleWriter = true,
        SingleReader = true   // 每个下游只 1 个消费者,可优化掉锁
    });

// 2.2 启动后台任务
var stats = new TickStats();
var dispatcherTask = DispatcherAsync(ingestCh.Reader, riskCh.Writer, statCh.Writer, cts.Token);
var riskTask       = RiskConsumerAsync(riskCh.Reader, cts.Token);
var statTask       = StatConsumerAsync(statCh.Reader, stats, cts.Token);

// 2.3 启动 ZeroMQ REP 服务端
using var server = new ResponseSocket();
server.Bind("tcp://*:5556");

Console.WriteLine($"[服务端] 已启动,监听 tcp://*:5556");
Console.WriteLine("[服务端] 内部管道: REP → Ingest → Dispatcher → [Risk / Stat]");
Console.WriteLine("[服务端] 等待 EA 请求... (Ctrl+C 退出)\n");

// ============================================================================
// 3. 主循环:ZeroMQ REQ-REP 同步处理
// ============================================================================
while (!cts.IsCancellationRequested)
{
    // 3.1 阻塞接收 ZeroMQ 请求
    string reqJson;
    try { reqJson = server.ReceiveFrameString(); }
    catch (Exception ex) when (ex is OperationCanceledException or NetMQException) { break; }

    // 3.2 解析 Tick
    var tick = ParseTick(reqJson);

    // 3.3 写入 Channel 给后台消费者(非阻塞,满了自动丢最旧)
    //     ★ 关键:不能 await,否则会阻塞 REP 响应导致 REQ 超时
    if (tick is not null)
        ingestCh.Writer.TryWrite(tick);

    // 3.4 立即响应(REQ-REP 协议必须立即返回)
    string respJson = BuildResponse(tick, ingestCh.Reader.Count);
    server.SendFrame(respJson);

    // 3.5 控制台输出(简洁)
    if (tick is not null)
        Console.WriteLine($"[收到] seq={tick.Seq,3} {tick.Symbol} @ {tick.Price:F5} | 队列:{ingestCh.Reader.Count,3}");
    else
        Console.WriteLine($"[收到] 解析失败: {reqJson}");
}

// 4. 优雅关闭:通知消费者不再有新数据
ingestCh.Writer.TryComplete();
Console.WriteLine("\n[服务端] 正在关闭...");

// 等待 Dispatcher 处理完 ingest 队列(它会自动级联关闭下游 Channel)
await dispatcherTask;

// 等待两个下游消费者处理完各自的队列
await Task.WhenAll(riskTask, statTask);

// 打印最终统计
Console.WriteLine("\n=== 最终统计 ===");
foreach (var (symbol, s) in stats.GetAll())
    Console.WriteLine($"  {symbol}: 处理 {s.Count} 条,均价 {s.AvgPrice:F5},最高 {s.MaxPrice:F5},最低 {s.MinPrice:F5}");
Console.WriteLine("[服务端] 已退出");


// ============================================================================
// 业务函数
// ============================================================================

// 解析 JSON 请求为 Tick 对象
static Tick? ParseTick(string json)
{
    try
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        return new Tick(
            Symbol: root.GetProperty("symbol").GetString() ?? "?",
            Price:  root.GetProperty("price").GetDouble(),
            Time:   DateTime.Parse(root.GetProperty("time").GetString() ?? DateTime.Now.ToString("o")),
            Seq:    root.TryGetProperty("seq", out var seq) ? seq.GetInt32() : 0
        );
    }
    catch { return null; }
}

// 构造 ZeroMQ 响应 JSON
static string BuildResponse(Tick? tick, int queueSize)
{
    if (tick is null)
        return JsonSerializer.Serialize(new { ok = false, msg = "解析失败" });

    return JsonSerializer.Serialize(new
    {
        ok = true,
        msg = $"已收到 {tick.Symbol} @ {tick.Price:F5}",
        server_time = DateTime.Now.ToString("o"),
        queue = queueSize
    });
}

// ============================================================================
// Dispatcher:从 ingest 读取,fan-out 到 risk + stat 两个 Channel
// ============================================================================
// 这是实现"一条 Tick 被多个消费者同时处理"的关键
// (Channel 原生多消费者是竞争模式,要 fan-out 必须 Dispatcher 拆分)
static async Task DispatcherAsync(
    ChannelReader<Tick> reader,
    ChannelWriter<Tick> riskWriter,
    ChannelWriter<Tick> statWriter,
    CancellationToken ct)
{
    try
    {
        await foreach (var tick in reader.ReadAllAsync(ct))
        {
            // 同时写入两个下游 Channel
            // 用 TryWrite 不阻塞,满了自动丢(因为 DropOldest 策略)
            riskWriter.TryWrite(tick);
            statWriter.TryWrite(tick);
        }
    }
    finally
    {
        // ingest Channel 关闭后,级联关闭下游 Channel
        riskWriter.TryComplete();
        statWriter.TryComplete();
    }
}

// ============================================================================
// 消费者 1:风控 - 检测异常价格
// ============================================================================
// 业务规则:
//   - EURUSD 正常范围 0.5 ~ 2.0
//   - XAUUSD 正常范围 100 ~ 5000
//   - USDJPY 正常范围 50 ~ 300
// 触发异常时输出告警
static async Task RiskConsumerAsync(ChannelReader<Tick> reader, CancellationToken ct)
{
    var ranges = new Dictionary<string, (double Min, double Max)>
    {
        ["EURUSD"] = (0.5, 2.0),
        ["XAUUSD"] = (100, 5000),
        ["USDJPY"] = (50, 300)
    };

    await foreach (var tick in reader.ReadAllAsync(ct))
    {
        if (!ranges.TryGetValue(tick.Symbol, out var range)) continue;

        if (tick.Price < range.Min || tick.Price > range.Max)
        {
            // 告警输出(实际项目可写日志/发邮件/触发熔断)
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"  [风控告警] seq={tick.Seq} {tick.Symbol} 异常价格 {tick.Price:F5} (正常 {range.Min}-{range.Max})");
            Console.ResetColor();
        }
    }
}

// ============================================================================
// 消费者 2:统计 - 按品种聚合均价/最高/最低
// ============================================================================
static async Task StatConsumerAsync(ChannelReader<Tick> reader, TickStats stats, CancellationToken ct)
{
    await foreach (var tick in reader.ReadAllAsync(ct))
    {
        stats.Update(tick.Symbol, tick.Price);
    }
}


// ============================================================================
// 线程安全的统计聚合类
// ============================================================================
public class TickStats
{
    private readonly ConcurrentDictionary<string, StatItem> _data = new();

    public void Update(string symbol, double price)
    {
        var item = _data.GetOrAdd(symbol, _ => new StatItem());
        lock (item)
        {
            item.Count++;
            item.Sum += price;
            item.MaxPrice = Math.Max(item.MaxPrice, price);
            item.MinPrice = item.MinPrice == 0 ? price : Math.Min(item.MinPrice, price);
        }
    }

    public IEnumerable<KeyValuePair<string, StatItem>> GetAll() => _data;

    public class StatItem
    {
        public int Count;
        public double Sum;
        public double MaxPrice;
        public double MinPrice;
        public double AvgPrice => Count == 0 ? 0 : Sum / Count;
    }
}

// ============================================================================
// 数据模型
// ============================================================================
public record Tick(string Symbol, double Price, DateTime Time, int Seq);
