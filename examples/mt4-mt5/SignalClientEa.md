# SignalClientEa.mq5 - MT5 EA 与 C# 服务对接最简示例

> 本文件是 MQL5 源码 `SignalClientEa.mq5` 的 Markdown 版本,便于在 GitHub 上阅读。
> 实际使用时,把下方代码块复制到 MT5 数据目录 `MQL5/Experts/SignalClientEa.mq5`,在 MetaEditor 中按 F7 编译。

## 目的

演示 MQL EA 如何通过 ZeroMQ 调用 C# 服务,不涉及下单业务,只展示通信流程。

## 依赖

- [mql-zmq](https://github.com/dingmaotu/mql-zmq)
  - `libzmq.dll` → `MT5\MQL5\Libraries\`
  - `Zmq.mqh` → `MT5\MQL5\Include\Zmq\`

## 启动顺序

1. 先启动 C# 服务: `dotnet run --project examples/MtBridge.Server`
2. 在 MT5 挂载本 EA 到任意图表(每 10 秒自动发送当前价到 C# 服务)

## 源码

```mql5
//+------------------------------------------------------------------+
//|                                       SignalClientEa.mq5         |
//|              华鑫融汇 - MT5 EA 与 C# 服务对接最简示例             |
//+------------------------------------------------------------------+
// 【目的】演示 MQL EA 如何通过 ZeroMQ 调用 C# 服务
//        不涉及下单业务,只展示通信流程
//
// 【依赖】
//   mql-zmq 库: https://github.com/dingmaotu/mql-zmq
//   1. 把 libzmq.dll 复制到 MT5 数据目录\MQL5\Libraries\
//   2. 把 Zmq.mqh 复制到 MT5 数据目录\MQL5\Include\Zmq\
//
// 【启动顺序】
//   1. 先启动 C# 服务: dotnet run --project examples/MtBridge.Server
//   2. 在 MT5 挂载本 EA 到任意图表(每根 K 线收盘后发一次行情)
//+------------------------------------------------------------------+

#include <Zmq/Zmq.mqh>

input string ServerEndpoint = "tcp://127.0.0.1:5556";  // C# 服务端地址
input int    SendIntervalSeconds = 10;                  // 发送间隔(秒)

ZmqSocket *g_reqSocket;  // ZeroMQ REQ 套接字
datetime g_lastSendTime = 0;

//+------------------------------------------------------------------+
int OnInit()
{
    g_reqSocket = new ZmqSocket(ZMQ_REQ);
    if(g_reqSocket == NULL)
    {
        Print("【错误】创建 ZmqSocket 失败");
        return INIT_FAILED;
    }

    // ★ ZMQ_REQ 严格请求-响应模式: 必须 send → recv 配对
    g_reqSocket.connect(ServerEndpoint);
    Print("【初始化成功】已连接 ", ServerEndpoint);
    return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    if(g_reqSocket != NULL)
    {
        g_reqSocket.disconnect(ServerEndpoint);
        delete g_reqSocket;
    }
}

//+------------------------------------------------------------------+
void OnTick()
{
    // 按时间间隔发送(避免每个 tick 都发,流量过大)
    if(TimeCurrent() - g_lastSendTime < SendIntervalSeconds) return;
    g_lastSendTime = TimeCurrent();

    // --- 构造 JSON 请求(用当前 Bid 价) ---
    double bid = SymbolInfoDouble(_Symbol, SYMBOL_BID);
    string json = "{";
    json += "\"symbol\":\"" + _Symbol + "\",";
    json += "\"price\":" + DoubleToString(bid, _Digits) + ",";
    json += "\"time\":\"" + TimeToString(TimeCurrent(), TIME_DATE|TIME_SECONDS) + "\"";
    json += "}";

    // --- 发送给 C# 服务 ---
    ZmqMsg reqMsg;
    reqMsg.setData(json);
    g_reqSocket.send(reqMsg, true);
    Print("【发送】", json);

    // --- 接收响应 ---
    ZmqMsg repMsg;
    if(g_reqSocket.recv(repMsg, true))
    {
        string reply = repMsg.getData();
        Print("【收到】", reply);
    }
    else
    {
        Print("【错误】未收到响应");
    }
}
```

## 关键点说明

| 概念 | 说明 |
|------|------|
| `ZMQ_REQ` | ZeroMQ 请求-响应模式,严格 send → recv 配对 |
| `input` 关键字 | 声明可在 MT5 UI 调整的参数 |
| `OnInit / OnDeinit / OnTick` | EA 三大生命周期:加载/卸载/每个报价 |
| `_Symbol / _Digits` | MQL 内置变量:当前品种代码 / 价格小数位数 |
| `SymbolInfoDouble(_Symbol, SYMBOL_BID)` | 获取当前买价 |
| `TimeCurrent()` | 服务器当前时间(秒级) |

## 通信协议

### 请求(EA → C# 服务)
```json
{"symbol":"EURUSD","price":1.1050,"time":"2026.07.18 10:00:00"}
```

### 响应(C# 服务 → EA)
```json
{"ok":true,"msg":"已收到 EURUSD @ 1.10500","server_time":"2026-07-18T10:00:01"}
```

## 调试

在 MT5 终端的"专家"标签页查看 `Print` 输出:
```
【初始化成功】已连接 tcp://127.0.0.1:5556
【发送】{"symbol":"EURUSD","price":1.10500,"time":"2026.07.18 10:00:00"}
【收到】{"ok":true,"msg":"已收到 EURUSD @ 1.10500","server_time":"..."}
```

如果未收到响应,检查:
1. C# 服务端是否已启动(`dotnet run --project examples/MtBridge.Server`)
2. 防火墙是否放行 5556 端口
3. `libzmq.dll` 是否复制到 `MQL5\Libraries\`
