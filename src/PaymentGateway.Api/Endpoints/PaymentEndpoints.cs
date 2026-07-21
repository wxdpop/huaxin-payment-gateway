using Microsoft.AspNetCore.Mvc;
using PaymentGateway.Application.Payments;
using PaymentGateway.Infrastructure.Metrics;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Endpoints;

/// <summary>
/// 支付端点 —— 发起支付 / 渠道回调 / 退款
/// 学习要点:
///   1. POST /pay: 发起支付,模拟渠道调用,返回支付链接
///   2. POST /callbacks/{channel}: 接收渠道回调,复用 HandleCallbackService
///   3. POST /refund: 退款,涉及分布式锁 + 乐观锁 + 账户冻结/扣减
///   4. 完整支付链路: 下单 → 发起支付 → 渠道回调 → 入账 → (可选)退款
/// </summary>
public static class PaymentEndpoints
{
    public static void MapPaymentEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1").WithTags("支付");

        // POST: 发起支付
        // 学习要点: 从 Pending → Paying,生成渠道订单号,创建支付记录
        group.MapPost("/orders/{id:long}/pay", async (
            long id,
            [FromBody] PayOrderRequest req,
            IPayOrderService payService,
            CancellationToken ct) =>
        {
            var command = new PayOrderCommand(id, req.ChannelCode);
            var result = await payService.PayAsync(command, ct);
            // ★ M6-3: 支付发起指标(按渠道分组: wechat/alipay/unionpay)
            PaymentMetrics.PaymentsTotal.WithLabels(command.ChannelCode).Inc();
            return Results.Ok(Result<PayOrderResult>.Ok(result));
        })
        .WithName("PayOrder")
        .WithSummary("发起支付(模拟渠道调用)")
        .Produces<Result<PayOrderResult>>(StatusCodes.Status200OK)
        .Produces<Result>(StatusCodes.Status400BadRequest);

        // POST: 渠道回调
        // 学习要点: 模拟微信/支付宝支付成功回调,触发 HandleCallbackService
        //   回调幂等: Redis 锁(channel_order_no) + DB 唯一约束 双重保障
        group.MapPost("/callbacks/{channel}", async (
            string channel,
            [FromBody] CallbackRequest req,
            HandleCallbackService callbackService,
            CancellationToken ct) =>
        {
            var request = new HandleCallbackRequest(
                ChannelCode: channel,
                ChannelOrderNo: req.ChannelOrderNo,
                ChannelTradeNo: req.ChannelTradeNo,
                Amount: req.Amount,
                PaidAt: DateTimeOffset.UtcNow,
                CallbackRaw: System.Text.Json.JsonSerializer.Serialize(req));
            var result = await callbackService.HandleAsync(request, ct);
            // ★ M6-3: 回调处理指标(按状态分组: success/alreadyhandled/processing)
            PaymentMetrics.CallbacksTotal.WithLabels(result.ToString().ToLowerInvariant()).Inc();
            return Results.Ok(Result<HandleCallbackResult>.Ok(result));
        })
        .WithName("PaymentCallback")
        .WithSummary("接收渠道支付回调")
        .Produces<Result<HandleCallbackResult>>(StatusCodes.Status200OK);

        // POST: 退款
        // 学习要点: 从 Paid → Refunded,涉及资金冻结 + 渠道退款 + 余额扣减
        group.MapPost("/orders/{id:long}/refund", async (
            long id,
            [FromBody] RefundRequest req,
            IRefundOrderService refundService,
            CancellationToken ct) =>
        {
            var command = new RefundOrderCommand(id, req.Reason);
            var result = await refundService.RefundAsync(command, ct);
            return Results.Ok(Result<RefundOrderResult>.Ok(result));
        })
        .WithName("RefundOrder")
        .WithSummary("订单退款")
        .Produces<Result<RefundOrderResult>>(StatusCodes.Status200OK)
        .Produces<Result>(StatusCodes.Status400BadRequest);
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
