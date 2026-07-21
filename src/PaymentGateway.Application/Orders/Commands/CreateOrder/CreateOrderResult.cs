namespace PaymentGateway.Application.Orders.Commands.CreateOrder;

/// <summary>
/// 创建订单返回结果
/// 学习要点: 命令返回值只包含"必须"信息(订单号/状态),详情用查询接口获取
/// </summary>
public record CreateOrderResult(
    long OrderId,
    string OrderNo,
    string Status,
    decimal Amount);
