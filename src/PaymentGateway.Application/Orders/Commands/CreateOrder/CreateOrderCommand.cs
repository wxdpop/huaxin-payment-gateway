namespace PaymentGateway.Application.Orders.Commands.CreateOrder;

/// <summary>
/// 创建订单命令 —— CQRS 的 Command 部分(写操作)
/// 学习要点:
///   1. 命令是"意图",只携带必要数据,不含业务逻辑(业务逻辑在应用服务 CreateOrderService)
///   2. 用 record 保证不可变性(命令创建后不应被修改)
///   3. 命令属性与 API 请求 DTO 解耦,API 层负责映射
///   4. 已移除 MediatR.IRequest 接口,改为纯 record
///      由 ICreateOrderService.CreateAsync 显式接收,调用链路更直观
/// </summary>
/// <param name="MerchantId">商户ID(从请求头/JWT 解析)</param>
/// <param name="OutTradeNo">商户订单号(商户内部唯一,用于幂等)</param>
/// <param name="Amount">金额(元)</param>
/// <param name="Subject">订单标题</param>
/// <param name="ChannelCode">指定支付渠道(可选,留空由路由模块选择)</param>
public record CreateOrderCommand(
    long MerchantId,
    string OutTradeNo,
    decimal Amount,
    string Subject,
    string? ChannelCode = null);
