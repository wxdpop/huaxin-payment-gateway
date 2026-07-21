using NetMQ;
using NetMQ.Sockets;
using System.Text.Json;

// ============================================================================
// MT 对接 C# 服务端最小示例
// ============================================================================
// 演示 EA 与 C# 服务端的最简对接流程:
//   1. EA(MQL) 通过 ZeroMQ 发送 JSON 行情请求
//   2. 本服务端(C#) 接收并返回 JSON 响应
//
// 协议:
//   请求: {"symbol":"EURUSD","price":1.1050,"time":"2026-07-18T10:00:00"}
//   响应: {"ok":true,"msg":"已收到 EURUSD @ 1.1050","server_time":"..."}
//
// 启动:
//   dotnet run --project examples/MtBridge.Server
// ============================================================================

using var server = new ResponseSocket();

// ★ Bind 监听 5556 端口,EA 连接 tcp://127.0.0.1:5556
server.Bind("tcp://*:5556");
Console.WriteLine($"[服务端] 已启动,监听 tcp://*:5556");
Console.WriteLine("等待 EA 请求... (Ctrl+C 退出)\n");

while (true)
{
    // 阻塞等待接收 EA 请求
    string reqJson = server.ReceiveFrameString();
    Console.WriteLine($"[收到] {reqJson}");

    // 简单解析 + 回响应(实际项目可在此调用算法库/ML 模型)
    string respJson = BuildResponse(reqJson);
    server.SendFrame(respJson);
    Console.WriteLine($"[回复] {respJson}\n");
}

static string BuildResponse(string reqJson)
{
    try
    {
        using var doc = JsonDocument.Parse(reqJson);
        string symbol = doc.RootElement.GetProperty("symbol").GetString() ?? "?";
        double price  = doc.RootElement.GetProperty("price").GetDouble();

        var resp = new
        {
            ok = true,
            msg = $"已收到 {symbol} @ {price:F5}",
            server_time = DateTime.Now.ToString("o")
        };
        return JsonSerializer.Serialize(resp);
    }
    catch (Exception ex)
    {
        return JsonSerializer.Serialize(new { ok = false, msg = ex.Message });
    }
}
