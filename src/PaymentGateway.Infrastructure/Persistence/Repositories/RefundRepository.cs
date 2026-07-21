using PaymentGateway.Domain.Refunds;
using SqlSugar;

namespace PaymentGateway.Infrastructure.Persistence.Repositories;

/// <summary>
/// 退款仓储实现 —— SqlSugar 实现 IRefundRepository
/// 学习要点: 同 OrderRepository/PaymentRepository 模式
///   ExecuteReturnIdentityAsync 返回自增 ID,需手动赋值给实体
/// </summary>
public class RefundRepository : IRefundRepository
{
    private readonly ISqlSugarClient _db;

    public RefundRepository(ISqlSugarClient db) => _db = db;

    public async Task<RefundRecord?> GetByIdAsync(long id, CancellationToken ct = default)
        => await _db.Queryable<RefundRecord>().FirstAsync(r => r.Id == id);

    public async Task<RefundRecord?> FindByRefundNoAsync(string refundNo, CancellationToken ct = default)
        => await _db.Queryable<RefundRecord>().FirstAsync(r => r.RefundNo == refundNo);

    public async Task<RefundRecord?> FindByOrderIdAsync(long orderId, CancellationToken ct = default)
        => await _db.Queryable<RefundRecord>().FirstAsync(r => r.OrderId == orderId);

    public async Task AddAsync(RefundRecord refund, CancellationToken ct = default)
    {
        // SqlSugar 5.x 不会自动回填自增 ID,需显式赋值
        refund.Id = await _db.Insertable(refund).ExecuteReturnIdentityAsync();
    }

    public void Update(RefundRecord refund) =>
        _db.Updateable(refund).ExecuteCommand();
}
