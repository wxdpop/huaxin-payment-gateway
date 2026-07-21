using PaymentGateway.Domain.Payments;
using SqlSugar;

namespace PaymentGateway.Infrastructure.Persistence.Repositories;

/// <summary>
/// 支付记录仓储实现 —— SqlSugar
/// </summary>
public class PaymentRepository : IPaymentRepository
{
    private readonly ISqlSugarClient _db;

    public PaymentRepository(ISqlSugarClient db) => _db = db;

    public async Task<PaymentRecord?> FindByChannelOrderNoAsync(
        string channelCode, string channelOrderNo, CancellationToken ct = default)
        => await _db.Queryable<PaymentRecord>()
            .FirstAsync(p => p.ChannelCode == channelCode && p.ChannelOrderNo == channelOrderNo);

    public async Task<PaymentRecord?> GetByIdAsync(long id, CancellationToken ct = default)
        => await _db.Queryable<PaymentRecord>().FirstAsync(p => p.Id == id);

    public async Task AddAsync(PaymentRecord record, CancellationToken ct = default)
    {
        // SqlSugar 5.x 不会自动回填自增 ID,需显式赋值
        record.Id = await _db.Insertable(record).ExecuteReturnIdentityAsync();
    }

    public void Update(PaymentRecord record) =>
        _db.Updateable(record).ExecuteCommand();
}
