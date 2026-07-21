using PaymentGateway.Domain.Accounts;

namespace PaymentGateway.Application.Accounts;

/// <summary>
/// 账户查询服务 —— CQRS Query 端
/// 学习要点: 查询服务不经过聚合根,直接读 DB(性能优化)
///   写操作走聚合根(保证不变量),读操作可直接 DTO 投影
/// </summary>
public class AccountQueryService
{
    private readonly IAccountRepository _accountRepository;

    public AccountQueryService(IAccountRepository accountRepository)
        => _accountRepository = accountRepository;

    /// <summary>按商户ID查询账户余额</summary>
    public async Task<AccountBalanceDto> GetByMerchantIdAsync(long merchantId, CancellationToken ct = default)
    {
        var account = await _accountRepository.GetByMerchantIdAsync(merchantId, ct)
            ?? throw new Shared.Exceptions.BusinessException($"商户 {merchantId} 账户不存在", "ACCOUNT_NOT_FOUND");

        return new AccountBalanceDto(
            account.Id,
            account.MerchantId,
            account.Balance.Value,
            account.FrozenAmount.Value,
            account.Version);
    }
}

/// <summary>账户余额 DTO</summary>
public record AccountBalanceDto(
    long AccountId,
    long MerchantId,
    decimal Balance,
    decimal FrozenAmount,
    long Version);
