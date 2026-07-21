using PaymentGateway.Domain.Shared;

namespace PaymentGateway.Application.Abstractions;

/// <summary>
/// 领域事件分发器接口
/// 学习要点:
///   1. 应用层在 IUnitOfWork.SaveChangesAsync 成功后调用 DispatchAsync
///   2. 事件分发失败不应影响主事务(已提交),记录日志即可
///   3. M1 阶段仅日志输出验证流转,M2 阶段引入进程内应用服务分发,M3 阶段扩展到 Kafka 异步分发
/// </summary>
public interface IDomainEventDispatcher
{
    /// <summary>分发聚合根上的所有领域事件,分发后清空</summary>
    Task DispatchAsync(IHasDomainEvents aggregate, CancellationToken ct = default);
}
