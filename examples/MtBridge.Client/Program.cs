using NetMQ;
using NetMQ.Sockets;
using System.Text.Json;
using System.Diagnostics;
using System.Threading.Channels;

// ============================================================================
// C# 测试客户端 - 模拟 EA 发送请求(无需 MT5 终端即可调试对接流程)
// ============================================================================
// 启动方式:
//   1. 先启动服务端:  dotnet run --project examples/MtBridge.Server
//   2. 再启动客户端:  dotnet run --project examples/MtBridge.Client
// ============================================================================
const int Total = 1_000_000;

Console.WriteLine("=== 高并发三种方式对比 ===\n");

// 方式 1：Task.Run
{
    var sw = Stopwatch.StartNew();
    int processed = 0;
    var tasks = new List<Task>(Total);
    for (int i = 0; i < Total; i++)
    {
        int item = i;
        tasks.Add(Task.Run(() => Interlocked.Increment(ref processed)));
    }
    await Task.WhenAll(tasks);
    sw.Stop();
    Console.WriteLine($"[Task.Run]         处理 {processed} 项，耗时 {sw.ElapsedMilliseconds} ms，" +
                      $"吞吐 {Total * 1000L / sw.ElapsedMilliseconds} ops/s");
}

// 方式 2：ThreadPool.QueueUserWorkItem
{
    var sw = Stopwatch.StartNew();
    int processed = 0;
    using var mre = new ManualResetEventSlim(false);
    int remaining = Total;
    for (int i = 0; i < Total; i++)
    {
        ThreadPool.QueueUserWorkItem(_ =>
        {
            Interlocked.Increment(ref processed);
            if (Interlocked.Decrement(ref remaining) == 0) mre.Set();
        });
    }
    mre.Wait();
    sw.Stop();
    Console.WriteLine($"[ThreadPool.QWI]   处理 {processed} 项，耗时 {sw.ElapsedMilliseconds} ms，" +
                      $"吞吐 {Total * 1000L / sw.ElapsedMilliseconds} ops/s");
}

// 方式 3：Channel<T>（单生产者单消费者）
{
    var sw = Stopwatch.StartNew();
    int processed = 0;
    var channel = Channel.CreateBounded<int>(10000);

    // 消费者
    var consumer = Task.Run(async () =>
    {
        await foreach (var _ in channel.Reader.ReadAllAsync())
            Interlocked.Increment(ref processed);
    });

    // 生产者
    for (int i = 0; i < Total; i++)
        await channel.Writer.WriteAsync(i);
    channel.Writer.Complete();

    await consumer;
    sw.Stop();
    Console.WriteLine($"[Channel<T>]       处理 {processed} 项，耗时 {sw.ElapsedMilliseconds} ms，" +
                      $"吞吐 {Total * 1000L / sw.ElapsedMilliseconds} ops/s");
}
Console.ReadKey();
using var client = new RequestSocket();
client.Connect("tcp://127.0.0.1:5556");

// 构造 3 个模拟请求
var requests = new[]
{
    new { symbol = "EURUSD", price = 1.1050, time = DateTime.Now.ToString("o") },
    new { symbol = "XAUUSD", price = 2415.30, time = DateTime.Now.ToString("o") },
    new { symbol = "USDJPY", price = 156.20, time = DateTime.Now.ToString("o") }
};

foreach (var req in requests)
{
    string json = JsonSerializer.Serialize(req);
    Console.WriteLine($"[发送] {json}");
    client.SendFrame(json);

    if (client.TryReceiveFrameString(TimeSpan.FromSeconds(3), out string? reply))
        Console.WriteLine($"[收到] {reply}\n");
    else
        Console.WriteLine("[超时] 3 秒内未收到响应\n");
}
