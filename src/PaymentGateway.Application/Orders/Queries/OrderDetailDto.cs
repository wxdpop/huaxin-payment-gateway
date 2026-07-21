namespace PaymentGateway.Application.Orders.Queries;

/// <summary>
/// 订单详情 DTO —— 查询返回的数据传输对象
/// 学习要点: DTO 与领域实体解耦,只暴露前端需要的字段,避免领域层泄露
/// </summary>
public record OrderDetailDto(
    long Id,
    string OrderNo,
    long MerchantId,
    string OutTradeNo,
    string? ChannelCode,
    string? ChannelOrderNo,
    string Subject,
    decimal Amount,
    string Status,
    DateTimeOffset CreatedAt,
    DateTimeOffset? PaidAt);
