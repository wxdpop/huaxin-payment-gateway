using NetMQ;
using NetMQ.Sockets;
using System.Text.Json;

// ============================================================================
// C# 测试客户端 - 模拟 EA 发送请求(无需 MT5 终端即可调试对接流程)
// ============================================================================
// 启动方式:
//   1. 先启动服务端:  dotnet run --project examples/MtBridge.Server
//   2. 再启动客户端:  dotnet run --project examples/MtBridge.Client
// ============================================================================

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
