// ============================================================================
// C# 模拟客户端 - 构造真实 Tick 流测试服务端业务管道
// ============================================================================
// 模拟内容:
//   1. 3 个品种的实时 Tick(EURUSD/XAUUSD/USDJPY)
//   2. 价格小幅波动 ±0.1%(模拟真实行情)
//   3. 每 20 条插入一条异常价格(测试服务端风控)
//   4. 按 100ms 间隔推送(模拟 Tick 节奏)
//
// 启动方式:
//   1. 先启动服务端:  dotnet run --project examples/MtBridge.Server
//   2. 再启动客户端:  dotnet run --project examples/MtBridge.Client
// ============================================================================

using NetMQ;
using NetMQ.Sockets;
using System.Text.Json;

// 1. 连接服务端
using var client = new RequestSocket();
client.Connect("tcp://127.0.0.1:5556");
Console.WriteLine("[客户端] 已连接 tcp://127.0.0.1:5556");
Console.WriteLine("[客户端] 开始推送 100 条模拟行情(含 5 条异常价)...\n");

// 2. 模拟数据生成器
//    每个品种有基准价,价格在 ±0.1% 范围内小幅波动(模拟真实 Tick)
var random = new Random(42);  // 固定种子,结果可重现
var symbols = new[] { "EURUSD", "XAUUSD", "USDJPY" };
var basePrices = new Dictionary<string, double>
{
    ["EURUSD"] = 1.1050,
    ["XAUUSD"] = 2415.30,
    ["USDJPY"] = 156.20
};

// 3. 推送 100 条 Tick
const int Total = 100;
var stats = new ClientStats();

for (int i = 0; i < Total; i++)
{
    // 3.1 选择品种(轮流)
    string symbol = symbols[i % symbols.Length];
    double basePrice = basePrices[symbol];

    // 3.2 价格波动 ±0.1%
    double drift = (random.NextDouble() - 0.5) * 0.002;
    double price = basePrice * (1 + drift);

    // 3.3 每 20 条插入一条异常价(测试风控)
    //   - 异常 1: 价格飙升到 99999(明显超出范围)
    //   - 异常 2: 价格为 0(数据错误)
    string? tag = null;
    if (i > 0 && i % 20 == 0)
    {
        // 交替两种异常
        price = (i / 20) % 2 == 0 ? 99999.0 : 0.0;
        tag = "异常";
    }

    // 3.4 构造请求
    var req = new
    {
        symbol,
        price,
        time = DateTime.Now.ToString("o"),
        seq = i
    };
    string json = JsonSerializer.Serialize(req);

    // 3.5 发送 + 接收响应
    Console.Write($"[{i,3:D3}] [发送] {symbol} @ {price,9:F5}{(tag is null ? "" : "  ← " + tag),-12}");
    client.SendFrame(json);

    if (client.TryReceiveFrameString(TimeSpan.FromSeconds(3), out string? reply))
    {
        // 解析响应里的 queue 字段,观察服务端队列堆积情况
        int queueSize = ParseQueueSize(reply);
        Console.WriteLine($" → [响应] 队列={queueSize,3}");
        stats.OnReply(true, queueSize);
    }
    else
    {
        Console.WriteLine(" → [超时]");
        stats.OnReply(false, 0);
    }

    // 3.6 按 100ms 间隔推送(模拟 Tick 节奏)
    Thread.Sleep(100);
}

// 4. 打印客户端统计
Console.WriteLine("\n=== 客户端统计 ===");
Console.WriteLine($"  发送: {stats.Sent} 条");
Console.WriteLine($"  成功: {stats.Success} 条");
Console.WriteLine($"  超时: {stats.Timeout} 条");
Console.WriteLine($"  最大队列堆积: {stats.MaxQueue}");
Console.WriteLine($"\n[客户端] 推送完毕");


// ============================================================================
// 工具函数
// ============================================================================

// 从响应 JSON 解析出队列大小(用于观察服务端管道是否堆积)
static int ParseQueueSize(string reply)
{
    try
    {
        using var doc = JsonDocument.Parse(reply);
        if (doc.RootElement.TryGetProperty("queue", out var q))
            return q.GetInt32();
    }
    catch { }
    return -1;
}

// ============================================================================
// 客户端统计
// ============================================================================
class ClientStats
{
    public int Sent { get; private set; }
    public int Success { get; private set; }
    public int Timeout { get; private set; }
    public int MaxQueue { get; private set; }

    public void OnReply(bool success, int queue)
    {
        Sent++;
        if (success) Success++; else Timeout++;
        if (queue > MaxQueue) MaxQueue = queue;
    }
}
