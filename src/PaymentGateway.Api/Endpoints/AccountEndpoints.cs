using Microsoft.AspNetCore.Mvc;
using PaymentGateway.Application.Accounts;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Endpoints;

/// <summary>
/// 账户端点 —— 查询商户资金账户余额
/// 学习要点: 账户是资金安全的核心,查询用 CQRS Query 端,变更走聚合根
/// </summary>
public static class AccountEndpoints
{
    public static void MapAccountEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/accounts").WithTags("账户");

        // GET: 按商户ID查询账户余额
        group.MapGet("/{merchantId:long}", async (
            long merchantId,
            AccountQueryService queryService,
            CancellationToken ct) =>
        {
            var dto = await queryService.GetByMerchantIdAsync(merchantId, ct);
            return Results.Ok(Result<AccountBalanceDto>.Ok(dto));
        })
        .WithName("GetAccountByMerchantId")
        .WithSummary("按商户ID查询账户余额");
    }
}
