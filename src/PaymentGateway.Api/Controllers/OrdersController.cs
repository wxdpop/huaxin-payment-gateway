using Microsoft.AspNetCore.Mvc;
using PaymentGateway.Application.Orders.Commands.CreateOrder;
using PaymentGateway.Application.Orders.Queries;
using PaymentGateway.Infrastructure.Metrics;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Controllers;

/// <summary>
/// 订单控制器 —— 传统 Controller 风格(原 Minimal API 的 OrderEndpoints)
/// 学习要点:
///   1. [ApiController] 启用: 自动 400 响应、[FromBody] 默认绑定、问题详情 RFC 7807
///   2. [Route] + [Tags] 对应 Minimal API 的 MapGroup + WithTags
///   3. [ProducesResponseType] 对应 Minimal API 的 Produces
///   4. 路由名(Name=) 对应 Minimal API 的 WithName
///   5. 已移除 MediatR: 直接注入 ICreateOrderService,编译期类型检查
/// </summary>
[ApiController]
[Route("api/v1/orders")]
[Tags("订单")]
public class OrdersController : ControllerBase
{
    private readonly ICreateOrderService _orderService;
    private readonly OrderQueryService _queryService;

    // 构造函数注入:学习要点 - Controller 通过 DI 注入应用服务,而不是 [FromServices] 单方法注入
    //   对比 Minimal API: 每个 MapPost 的参数列表里都要声明 [FromServices] ICreateOrderService
    public OrdersController(ICreateOrderService orderService, OrderQueryService queryService)
    {
        _orderService = orderService;
        _queryService = queryService;
    }

    // POST: 创建订单
    //   学习要点: 从 [FromBody] 接收 DTO,映射为 Command,调用应用服务
    //   ★ [FromBody] 在 [ApiController] 模式下可省略(复杂类型默认 body 绑定),这里显式声明保留原意
    /// <summary>
    /// 创建支付订单
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(Result<CreateOrderResult>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateOrder(
        [FromBody] CreateOrderRequest req,
        CancellationToken ct)
    {
        var command = new CreateOrderCommand(
            req.MerchantId, req.OutTradeNo, req.Amount, req.Subject, req.ChannelCode);
        var result = await _orderService.CreateAsync(command, ct);
        // ★ M6-3: 订单创建指标(单调递增 Counter)
        PaymentMetrics.OrdersCreatedTotal.Inc();
        return Ok(Result<CreateOrderResult>.Ok(result));
    }

    // GET: 按订单 ID 查询
    /// <summary>
    /// 按订单ID查询
    /// </summary>
    [HttpGet("{id:long}", Name = "GetOrderById")]
    [ProducesResponseType(typeof(Result<OrderDetailDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetById(long id, CancellationToken ct)
    {
        var dto = await _queryService.GetByIdAsync(id, ct);
        return Ok(Result<OrderDetailDto>.Ok(dto));
    }

    // GET: 按平台订单号查询
    /// <summary>
    /// 按平台订单号查询
    /// </summary>
    [HttpGet("by-no/{orderNo}", Name = "GetOrderByOrderNo")]
    [ProducesResponseType(typeof(Result<OrderDetailDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetByOrderNo(string orderNo, CancellationToken ct)
    {
        var dto = await _queryService.GetByOrderNoAsync(orderNo, ct);
        return Ok(Result<OrderDetailDto>.Ok(dto));
    }
}

/// <summary>
/// 创建订单请求 DTO —— 与 Command 解耦
/// 学习要点: API DTO 接收前端原始数据,在应用服务中映射为 Command
/// </summary>
public record CreateOrderRequest(
    long MerchantId,
    string OutTradeNo,
    decimal Amount,
    string Subject,
    string? ChannelCode = null);
