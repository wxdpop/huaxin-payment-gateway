using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Domain.Shared;

namespace PaymentGateway.Infrastructure.Common;

/// <summary>
/// 领域事件分发器实现 —— M1 阶段仅记录日志
/// 学习要点:
///   1. M1 阶段: 仅日志输出,验证事件流转正确
///   2. M2 阶段: 扩展为通过应用服务接口分发(进程内同步,无 MediatR)
///   3. M3 阶段: 扩展为通过 Kafka 异步分发(跨服务解耦)
///   4. 事件分发失败仅记录日志,不影响主事务(事务已提交)
/// </summary>
public class DomainEventDispatcher : IDomainEventDispatcher
{
    private readonly ILogger<DomainEventDispatcher> _logger;

    public DomainEventDispatcher(ILogger<DomainEventDispatcher> logger) => _logger = logger;

    public async Task DispatchAsync(IHasDomainEvents aggregate, CancellationToken ct = default)
    {
        var events = aggregate.DomainEvents.ToList();
        if (events.Count == 0) return;

        foreach (var evt in events)
        {
            // M1 阶段: 仅日志记录事件,验证流转正确
            _logger.LogInformation(
                "领域事件分发: EventType={EventType}, EventId={EventId}, OccurredAt={OccurredAt}",
                evt.GetType().Name, evt.EventId, evt.OccurredAt);
        }

        aggregate.ClearDomainEvents();
        await Task.CompletedTask;
    }
}
