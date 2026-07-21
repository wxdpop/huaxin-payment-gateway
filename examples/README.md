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

---

## 7. 面试知识点速查

### MT4 vs MT5 核心区别
| 维度 | MT4 | MT5 |
|------|-----|-----|
| 发布年份 | 2005 | 2010 |
| 语言 | MQL4 | MQL5 (面向对象) |
| 持仓模型 | 单向(可对冲) | 双向(多空分别记账) |
| 回测 | 单线程 | 多线程+多货币 |
| 市场地位 | 零售外汇主流 | 新一代全资产 |
| 经纪商授权 | 已停发新授权 | 主推 |

### EA 生命周期三大函数
- `OnInit()` - 加载时调用一次(初始化参数、句柄)
- `OnTick()` - 每收到新报价(Tick)调用(策略主逻辑)
- `OnDeinit(reason)` - 卸载时调用一次(释放资源)

### MT4 vs MT5 API 差异
| 操作 | MT4 | MT5 |
|------|-----|-----|
| 取收盘价 | `Close[1]` 内置数组 | `iClose(_Symbol,_Period,1)` 函数 |
| 计算指标 | `iMA(...)` 直接返回值 | `iMA(...)` 返回句柄 + `CopyBuffer` 取值 |
| 下单 | `OrderSend()` 统一函数 | `CTrade` 类封装 |
| 持仓查询 | `OrderSelect` 遍历 `OrdersTotal` | `PositionGetTicket` 遍历 `PositionsTotal` |

### 常见下单错误码
| 错误码 | 含义 | 排查 |
|--------|------|------|
| 130 | 止损止盈距离过近 | 检查 `SYMBOL_TRADE_STOPS_LEVEL` |
| 134 | 余额不足 | 查 `AccountFreeMargin()` |
| 138 | 价格已变(重报价) | 加大 slippage 或改挂单 |
| 133 | 交易被禁 | 联系经纪商 |

### 关键概念
- **Bid/Ask**: 买价/卖价,Ask - Bid = 点差(经纪商利润)
- **Point/Digits**: 最小变动单位/小数位数(EURUSD Point=0.00001)
- **Pip**: 1 pip = 10 Point
- **Magic Number**: EA 标识,区分自己下的单(避免误删别人持仓)
- **Slippage**: 允许的价格偏差(点数),防重报价失败

### 部署架构
```
策略服务器(VPS/云,低延迟)
    │ TCP/UDP
    ▼
经纪商服务器
    │
    ▼
交易中心(下单路由)
```
- VPS 选址:离经纪商服务器近(纽约/伦敦/阿姆斯特丹)
- 网络要求:延迟 < 10ms,丢包 < 0.1%
- 监控要点:EA 进程存活、连接状态、持仓异常

---

## 8. 面试话术

### Q1: 你做过 MT4/MT5 开发吗?能讲讲你的项目?

> 我做了一个 **MT5 EA 与 C# 服务对接**的工程。EA 端用 MQL5 写,负责接收行情、转发给 C# 服务,然后根据 C# 返回的信号下单。C# 服务端用 .NET 8 + NetMQ(ZeroMQ 的 C# 实现) 实现,负责算法计算。两者通过 ZeroMQ 用 JSON 协议通信。
>
> 之所以这样设计,是因为 MQL 算法库少、调试简陋,而 C# 可以用 NuGet 海量库(ML.NET、MathNet、ONNX 推理),VS2022 调试器远胜 MetaEditor。一次开发,多个 EA 共享同一个 C# 服务。

### Q2: 为什么选 ZeroMQ 而不是 HTTP/gRPC?

> 三个原因:
> 1. **延迟**: ZeroMQ 微秒级,HTTP 毫秒级,交易场景对延迟敏感
> 2. **依赖**: ZeroMQ 单文件 libzmq.dll,无需 IIS/Nginx,部署简单
> 3. **互操作**: MQL 端通过 mql-zmq 库可直接连接,而 gRPC 在 MQL 中没有现成实现

### Q3: MT4 和 MT5 有什么区别?

> 主要是 **持仓模型** 和 **API 设计** 两个差异:
> - MT4 是单向持仓(同一品种只能有一个方向),MT5 是双向(多空分别记账)
> - MT4 的 iMA 直接返回值,MT5 改成句柄 + CopyBuffer 模式(更接近现代 API 设计)
> - MT4 的下单用 OrderSend 统一函数,MT5 用 CTrade 类封装,面向对象更清晰
>
> 另外 MT5 支持多线程回测和经济日历,MT4 没有。市场地位上,MT4 在零售外汇仍是主流,但 MetaQuotes 已停止为 MT4 提供新经纪商授权,所以 MT5 是未来趋势。

### Q4: EA 生命周期是怎样的?

> 三个核心函数:
> - **OnInit**: EA 加载时调用一次,做参数校验、指标句柄创建、初始化
> - **OnTick**: 每收到一个新报价(Tick)调用一次,策略主逻辑在这里。优化时通常只在新 K 线收盘时执行,避免一个 K 线内频繁下单
> - **OnDeinit**: 卸载时调用一次,释放指标句柄等系统资源
>
> Tick 是价格最小变动单位,1 分钟内可能触发几十次 OnTick,所以高频策略要特别注意性能。

### Q5: 你的 EA 怎么区分自己下的单?

> 用 **Magic Number**。每个 EA 在初始化时设置一个唯一的魔数(比如时间戳),下单时 CTrade.SetExpertMagicNumber 把这个魔数标记到订单上。平仓/查询时按魔数过滤,只动自己下的单,避免误删别人持仓。
>
> 这在多 EA 共享一个账户时尤其重要。

### Q6: 下单失败怎么排查?

> 看错误码:
> - **130**: 止损止盈距离过近,要查 `SYMBOL_TRADE_STOPS_LEVEL` 最小距离
> - **134**: 余额不足,查 `AccountFreeMargin()`
> - **138**: 价格已变(重报价),加大 slippage 或改用挂单
> - **133**: 交易被禁,联系经纪商
>
> 另外,MT 终端顶部的"自动交易"按钮必须点亮(绿色),否则 EA 不会下单。

### Q7: 怎么做风控?

> 三层防护:
> 1. **单笔风控**: 每单风险 = AccountBalance × 1% / (StopLoss_Point × PointValue),仓位固定比例
> 2. **总持仓风控**: 净值回撤超 20% 暂停交易
> 3. **运维风控**: 监控 EA 进程存活、连接状态、持仓异常,断线自动重连
>
> 资金管理常用凯利公式: f = (p × b - q) / b,p=胜率,b=盈亏比,q=1-p。

### Q8: 怎么部署 EA 到生产?

> 标准流程:
> 1. **回测**: 用历史 K 线验证策略盈利性(每 Tick / 1 分钟 / 仅开盘价 三种精度)
> 2. **模拟账户**: 实时数据 + 模拟资金,验证 EA 真实行为 1-3 个月
> 3. **小资金实盘**: 0.01 手试运行
> 4. **逐步加仓**: 表现稳定后增加手数
>
> 部署位置选 VPS,离经纪商服务器近(纽约/伦敦/阿姆斯特丹),延迟 < 10ms。

### Q9: 你们 EA 信号怎么生成的?

> 多指标综合评分:
> - MA 金叉/死叉(趋势核心,权重 40%)
> - RSI 超买超卖(动量确认,权重 30%)
> - MACD 柱状图(动能方向,权重 30%)
>
> 评分 > 50 买入, < -50 卖出,其他持有。单一指标容易误判(金叉死叉频繁假信号),多指标综合过滤更可靠。

### Q10: 你们用什么监控?

> 三层监控:
> 1. **EA 进程**: MT 终端意外关闭 → 自动重启 + 重新加载 EA
> 2. **连接状态**: `TerminalInfoInteger(TERMINAL_CONNECTED)` 检测断线
> 3. **持仓异常**: 突然增多/减少 → 告警(可能经纪商强平或人工干预)
>
> 日志按天归档到 ELK,Prometheus 抓取 /metrics,Grafana 展示。
