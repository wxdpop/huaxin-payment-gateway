using Microsoft.AspNetCore.Mvc;
using PaymentGateway.Application.Orders.Commands.CreateOrder;
using PaymentGateway.Application.Orders.Queries;
using PaymentGateway.Infrastructure.Metrics;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Endpoints;

/// <summary>
/// 订单端点 —— Minimal API 风格的路由定义
/// 学习要点:
///   1. .NET 8 推荐 Minimal API(轻量),也可用 Controller(传统)
///   2. MapPost/MapGet 自动绑定参数([FromBody] / [FromQuery] / [AsParameters])
///   3. 用 IResult 返回标准化响应(也可直接返回 Result,统一封装在 ExceptionMiddleware)
///   4. 路由前缀 /api/v1/orders,版本号 v1 便于后续演进
///   5. 已移除 MediatR: 不再注入 IMediator.Send,改为直接注入 ICreateOrderService.CreateAsync
///      调用链路更直观,且更利于单元测试 Mock
/// </summary>
public static class OrderEndpoints
{
    public static void MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/orders").WithTags("订单");

        // POST: 创建订单
        // 学习要点: Minimal API 直接注入 ICreateOrderService(应用服务接口)
        //   相比 IMediator.Send 的"反射路由",这里显式调用 Service,编译期即可检查类型
        group.MapPost("/", async (
            [FromBody] CreateOrderRequest req,
            ICreateOrderService orderService,
            CancellationToken ct) =>
        {
            var command = new CreateOrderCommand(
                req.MerchantId, req.OutTradeNo, req.Amount, req.Subject, req.ChannelCode);
            var result = await orderService.CreateAsync(command, ct);
            // ★ M6-3: 订单创建指标(单调递增 Counter)
            PaymentMetrics.OrdersCreatedTotal.Inc();
            return Results.Ok(Result<CreateOrderResult>.Ok(result));
        })
        .WithName("CreateOrder")
        .WithSummary("创建支付订单")
        .Produces<Result<CreateOrderResult>>(StatusCodes.Status200OK)
        .Produces<Result>(StatusCodes.Status400BadRequest);

        // GET: 按ID查询订单
        group.MapGet("/{id:long}", async (
            long id,
            OrderQueryService queryService,
            CancellationToken ct) =>
        {
            var dto = await queryService.GetByIdAsync(id, ct);
            return Results.Ok(Result<OrderDetailDto>.Ok(dto));
        })
        .WithName("GetOrderById")
        .WithSummary("按订单ID查询");

        // GET: 按订单号查询订单
        group.MapGet("/by-no/{orderNo}", async (
            string orderNo,
            OrderQueryService queryService,
            CancellationToken ct) =>
        {
            var dto = await queryService.GetByOrderNoAsync(orderNo, ct);
            return Results.Ok(Result<OrderDetailDto>.Ok(dto));
        })
        .WithName("GetOrderByOrderNo")
        .WithSummary("按平台订单号查询");
    }
}

/// <summary>
/// 创建订单请求 DTO —— API 层定义,与 Command 解耦
/// 学习要点: API DTO 接收前端原始数据,在应用服务中映射为 Command
/// </summary>
public record CreateOrderRequest(
    long MerchantId,
    string OutTradeNo,
    decimal Amount,
    string Subject,
    string? ChannelCode = null);
