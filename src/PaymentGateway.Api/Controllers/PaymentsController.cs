using Microsoft.AspNetCore.Mvc;
using PaymentGateway.Application.Payments;
using PaymentGateway.Infrastructure.Metrics;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Controllers;

/// <summary>
/// 支付控制器 —— 发起支付 / 渠道回调 / 退款
/// 学习要点(原 Minimal API 的 PaymentEndpoints 改造):
///   1. POST /pay: 发起支付,模拟渠道调用,返回支付链接
///   2. POST /callbacks/{channel}: 接收渠道回调,复用 HandleCallbackService
///   3. POST /refund: 退款,涉及分布式锁 + 乐观锁 + 账户冻结/扣减
///   4. 完整支付链路: 下单 → 发起支付 → 渠道回调 → 入账 → (可选)退款
/// </summary>
[ApiController]
[Route("api/v1")]
[Tags("支付")]
public class PaymentsController : ControllerBase
{
    private readonly IPayOrderService _payService;
    private readonly HandleCallbackService _callbackService;
    private readonly IRefundOrderService _refundService;

    // 构造函数注入三个服务
    public PaymentsController(
        IPayOrderService payService,
        HandleCallbackService callbackService,
        IRefundOrderService refundService)
    {
        _payService = payService;
        _callbackService = callbackService;
        _refundService = refundService;
    }

    // POST: 发起支付
    //   学习要点: 从 Pending → Paying,生成渠道订单号,创建支付记录
    /// <summary>
    /// 发起支付(模拟渠道调用)
    /// </summary>
    [HttpPost("orders/{id:long}/pay", Name = "PayOrder")]
    [ProducesResponseType(typeof(Result<PayOrderResult>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> PayOrder(
        [FromRoute] long id,
        [FromBody] PayOrderRequest req,
        CancellationToken ct)
    {
        var command = new PayOrderCommand(id, req.ChannelCode);
        var result = await _payService.PayAsync(command, ct);
        // ★ M6-3: 支付发起指标(按渠道分组: wechat/alipay/unionpay)
        PaymentMetrics.PaymentsTotal.WithLabels(command.ChannelCode).Inc();
        return Ok(Result<PayOrderResult>.Ok(result));
    }

    // POST: 渠道回调
    //   学习要点: 模拟微信/支付宝支付成功回调,触发 HandleCallbackService
    //     回调幂等: Redis 锁(channel_order_no) + DB 唯一约束 双重保障
    /// <summary>
    /// 接收渠道支付回调
    /// </summary>
    [HttpPost("callbacks/{channel}", Name = "PaymentCallback")]
    [ProducesResponseType(typeof(Result<HandleCallbackResult>), StatusCodes.Status200OK)]
    public async Task<IActionResult> Callback(
        [FromRoute] string channel,
        [FromBody] CallbackRequest req,
        CancellationToken ct)
    {
        var request = new HandleCallbackRequest(
            ChannelCode: channel,
            ChannelOrderNo: req.ChannelOrderNo,
            ChannelTradeNo: req.ChannelTradeNo,
            Amount: req.Amount,
            PaidAt: DateTimeOffset.UtcNow,
            CallbackRaw: System.Text.Json.JsonSerializer.Serialize(req));
        var result = await _callbackService.HandleAsync(request, ct);
        // ★ M6-3: 回调处理指标(按状态分组: success/alreadyhandled/processing)
        PaymentMetrics.CallbacksTotal.WithLabels(result.ToString().ToLowerInvariant()).Inc();
        return Ok(Result<HandleCallbackResult>.Ok(result));
    }

    // POST: 退款
    //   学习要点: 从 Paid → Refunded,涉及资金冻结 + 渠道退款 + 余额扣减
    /// <summary>
    /// 订单退款
    /// </summary>
    [HttpPost("orders/{id:long}/refund", Name = "RefundOrder")]
    [ProducesResponseType(typeof(Result<RefundOrderResult>), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Result), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Refund(
        [FromRoute] long id,
        [FromBody] RefundRequest req,
        CancellationToken ct)
    {
        var command = new RefundOrderCommand(id, req.Reason);
        var result = await _refundService.RefundAsync(command, ct);
        return Ok(Result<RefundOrderResult>.Ok(result));
    }
}

// ============================================================================
// 请求 DTO
// ============================================================================

/// <summary>发起支付请求</summary>
public record PayOrderRequest(string ChannelCode);

/// <summary>渠道回调请求(模拟微信/支付宝回调报文)</summary>
public record CallbackRequest(
    string ChannelOrderNo,
    string? ChannelTradeNo,
    decimal Amount);

/// <summary>退款请求</summary>
public record RefundRequest(string? Reason);
