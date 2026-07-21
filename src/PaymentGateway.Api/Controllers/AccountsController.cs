using Microsoft.AspNetCore.Mvc;
using PaymentGateway.Application.Accounts;
using PaymentGateway.Shared.Results;

namespace PaymentGateway.Api.Controllers;

/// <summary>
/// 账户控制器 —— 查询商户资金账户余额
/// 学习要点(原 Minimal API 的 AccountEndpoints 改造):
///   账户是资金安全的核心,查询用 CQRS Query 端,变更走聚合根
/// </summary>
[ApiController]
[Route("api/v1/accounts")]
[Tags("账户")]
public class AccountsController : ControllerBase
{
    private readonly AccountQueryService _queryService;

    public AccountsController(AccountQueryService queryService)
    {
        _queryService = queryService;
    }

    // GET: 按商户 ID 查询账户余额
    /// <summary>
    /// 按商户ID查询账户余额
    /// </summary>
    [HttpGet("{merchantId:long}", Name = "GetAccountByMerchantId")]
    [ProducesResponseType(typeof(Result<AccountBalanceDto>), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetByMerchantId([FromRoute] long merchantId, CancellationToken ct)
    {
        var dto = await _queryService.GetByMerchantIdAsync(merchantId, ct);
        return Ok(Result<AccountBalanceDto>.Ok(dto));
    }
}
