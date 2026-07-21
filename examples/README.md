# MT4/MT5 对接示例

## 1. MT4/MT5 是什么

**MetaTrader 4/5** 是俄罗斯 MetaQuotes 公司开发的**外汇零售交易平台**,全球 80%+ 零售外汇经纪商使用。

| 维度 | MT4 (2005) | MT5 (2010) |
|------|------------|------------|
| 开发语言 | MQL4 (类 C++) | MQL5 (面向对象增强) |
| 文件 | `.mq4` → `.ex4` | `.mq5` → `.ex5` |
| 市场地位 | 老牌主流(零售外汇) | 新一代(外汇+股票+期货) |
| 程序类型 | EA / 指标 / 脚本 | 同 MT4 + 多线程回测 |

## 2. 能做什么

| 程序类型 | 业务场景 |
|---------|---------|
| **EA (Expert Advisor)** | 自动化交易:双均线/网格/马丁/套利 |
| **Custom Indicator** | 自定义指标画图:多周期 RSI、背离检测 |
| **Script** | 一次性工具:批量平仓、数据导出 |
| **DLL 调用** | EA 调用 C#/C++ 算法库或外部服务 |

**常见业务**:
- 自动化交易(占比最高)
- 跟单系统(主账户 → 多子账户)
- 风控监控(净值/保证金/持仓异常告警)
- 数据归档(K 线/Tick 落库做大数据分析)
- 跨语言集成(对接 ML 模型、Web 后端)

## 3. 如何对接

### 方式 1:纯 MQL 实现 EA
直接用 MQL 写策略,编译后挂载到 MT 终端。
- **优点**:无需外部依赖
- **缺点**:算法库少,调试简陋,无法用 ML

### 方式 2:EA + C# 服务(推荐)
EA 只做行情转发 + 下单,C# 服务做算法/业务,两者用 ZeroMQ 通信。

```
┌──────────────┐  ZeroMQ JSON   ┌──────────────┐
│  MT5 + EA    │ ──行情──>      │  C# 服务端   │
│  (REQ 端)    │ <─响应────      │  (REP 端)    │
└──────────────┘                └──────────────┘
```

**为什么这样设计?**
- C# 可用 NuGet 海量库(ML.NET、MathNet、ONNX 推理)
- VS2022 调试器远胜 MetaEditor
- 一次开发,MT4/MT5 多个 EA 共享

### 方式 3:Manager API 直连(企业级)
经纪商授权后用 `MetaTrader5.Manager` NuGet 直连 MT5 服务器,无需 EA 中转。
- **适用**:PAMM/MAM 跟单、风控平台

## 4. 快速开始(方式 2 对接示例)

### 4.1 不依赖 MT 终端验证对接流程
```bash
cd c:\Users\ZJN\Desktop\jl\project

# 终端 1: 启动 C# 服务端
dotnet run --project examples/MtBridge.Server

# 终端 2: 启动 C# 测试客户端(模拟 EA 发送行情)
dotnet run --project examples/MtBridge.Client
```

期望输出:
```
[发送] {"symbol":"EURUSD","price":1.105,"time":"2026-07-18..."}
[收到] {"ok":true,"msg":"已收到 EURUSD @ 1.10500","server_time":"..."}
```

### 4.2 真实接入 MT5 终端
1. 安装 [mql-zmq](https://github.com/dingmaotu/mql-zmq)
   - `libzmq.dll` → `MT5\MQL5\Libraries\`
   - `Zmq.mqh` → `MT5\MQL5\Include\Zmq\`
2. 启动 C# 服务:`dotnet run --project examples/MtBridge.Server`
3. 参考 [mt4-mt5/SignalClientEa.md](mt4-mt5/SignalClientEa.md) 中的 MQL5 源码,复制到 `MT5\MQL5\Experts\SignalClientEa.mq5`,按 F7 编译
4. 在 MT5 挂载 EA 到任意图表,每 10 秒自动发送当前价到 C# 服务

## 5. 项目结构

```
examples/
├── MtBridge.Server/         # C# ZeroMQ 服务端(最简对接示例)
│   ├── Program.cs
│   └── MtBridge.Server.csproj
├── MtBridge.Client/          # C# 测试客户端(不依赖 MT 即可调试)
│   ├── Program.cs
│   └── MtBridge.Client.csproj
├── mt4-mt5/
│   └── SignalClientEa.md     # MQL EA 端源码(Markdown 版,GitHub 可读)
└── README.md                 # 本文件
```

## 6. 协议

### 请求(EA → C# 服务)
```json
{"symbol":"EURUSD","price":1.1050,"time":"2026-07-18T10:00:00"}
```

### 响应(C# 服务 → EA)
```json
{"ok":true,"msg":"已收到 EURUSD @ 1.10500","server_time":"2026-07-18T10:00:01"}
```

> 实际项目中,可在 C# 服务端扩展任意算法/业务,响应字段自由定义。
