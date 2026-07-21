using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Infrastructure.DistributedLock.Redis;
using PaymentGateway.Infrastructure.DistributedLock.ZooKeeper;
using PaymentGateway.Infrastructure.Metrics;
using PaymentGateway.Infrastructure.Tracing;
using System.Diagnostics;

namespace PaymentGateway.Infrastructure.DistributedLock;

// ============================================================================
// DualLockProvider —— 双重分布式锁 (Redis 快锁 + ZK 强一致锁)
// ============================================================================
// ★ 学习要点: 这是支付场景资金安全的核心设计,工程亮点
//
// 【为什么需要"双重锁"?】
//   痛点: 单一锁都有缺陷
//   - Redis Redlock: AP 系统,主从切换期间可能丢锁 (网络分区时锁可能失效)
//   - ZooKeeper: CP 系统强一致,但性能差 (每次加锁需 ZAB 协议广播,延迟高)
//
//   解决方案: 两次加锁,各司其职
//   - 第一道 Redis 锁 (快锁): 拦截 99% 的重复请求 (高并发场景 QPS 500-2000)
//   - 第二道 ZK 锁 (强一致): 资金变更前最后把关,确保强一致 (支付成功入账)
//
// 【典型应用场景: 支付回调幂等 + 资金账户变更】
//   场景: 用户支付成功后,微信/支付宝可能因网络重试多次回调同一订单
//   流程:
//     1. 回调请求进入 → 获取 Redis 锁 (以 channel_order_no 为 key,5s 过期)
//        - 99% 的重复回调被拦截在这里 (Redis 快速失败)
//     2. Redis 锁获取成功 → 进行业务校验 (订单状态/支付记录)
//     3. 准备变更账户余额前 → 获取 ZK 锁 (以 merchant_id 为 key)
//        - ZK 强一致锁保护资金变更的临界区
//        - 同一商户的并发入账串行化,避免脏读与多记
//     4. ZK 锁获取成功 → 执行账户余额变更 (Credit/Debit) + 写流水
//     5. 释放 ZK 锁 → 释放 Redis 锁
//
// 【释放顺序: 后加的先释放】
//   学习要点: 释放顺序与加锁顺序相反,避免"释放后又被他人抢锁"导致业务未完成
//   1. 释放 ZK 锁 (后加的先释放,此时 Redis 锁还在,其他实例进不来)
//   2. 释放 Redis 锁 (业务完全结束,允许下一个回调进入)
// ============================================================================

public class DualLockProvider : IDistributedLock
{
    private readonly RedLockProvider _redisLock;
    private readonly ZooKeeperLockProvider _zkLock;
    private readonly ILogger<DualLockProvider> _logger;

    public DualLockProvider(
        RedLockProvider redisLock,
        ZooKeeperLockProvider zkLock,
        ILogger<DualLockProvider> logger)
    {
        _redisLock = redisLock;
        _zkLock = zkLock;
        _logger = logger;
    }

    /// <summary>
    /// 双重锁获取 (Redis 先锁,成功后 ZK 后锁)
    /// </summary>
    /// <remarks>
    /// 学习要点: "先 Redis 后 ZK" 顺序的考量
    ///   1. Redis 锁快 (毫秒级),先过滤掉重复请求,避免无意义的 ZK 调用
    ///   2. ZK 锁慢 (ZAB 协议需多数派共识,几十毫秒),放后面降低对 ZK 的压力
    ///   3. Redis 锁失败立即返回,不触发 ZK 调用 → 极大降低 ZK 集群负载
    /// </remarks>
    public async Task<ILockHandle?> TryAcquireAsync(
        string resource,
        TimeSpan expiryTime,
        CancellationToken ct = default)
    {
        // ★ M6-3: 链路追踪 Span + Prometheus 指标埋点
        //   学习要点: 关键业务路径同时埋 Span(查问题)和指标(看趋势)
        //   span 用 using 自动结束,指标用 Stopwatch 计时
        using var span = TraceContext.StartSpan("lock.dual.acquire", ("resource", resource));
        var sw = Stopwatch.StartNew();

        // ★ 第一道: Redis 快锁 (以 channel_order_no 为 key)
        var redisHandle = await _redisLock.TryAcquireAsync(resource, expiryTime, ct);
        if (redisHandle == null)
        {
            // 学习要点: 日志记录"快锁拦截"便于排查
            //   99% 的重复回调在这里被拦截,极大降低 ZK 负载
            _logger.LogInformation("双重锁: Redis 快锁拦截重复请求, resource={Resource}", resource);
            PaymentMetrics.LockAcquireTotal.WithLabels("dual", "fail").Inc();
            PaymentMetrics.LockAcquireDurationSeconds.WithLabels("dual").Observe(sw.Elapsed.TotalSeconds);
            TraceContext.AddEvent("redis_lock_blocked");
            return null;
        }

        // ★ 第二道: ZK 强一致锁 (以 merchant_id 为 key,这里简化为同一 resource)
        //   生产场景: Redis 用 channel_order_no 作 key (防回调重复),
        //            ZK 用 merchant_id 作 key (防资金并发变更)
        //   本学习工程: 简化为同一 resource,便于演示双重锁链路
        ILockHandle? zkHandle = null;
        try
        {
            zkHandle = await _zkLock.TryAcquireAsync(resource, expiryTime, ct);
            if (zkHandle == null)
            {
                // ★ Redis 锁获取成功但 ZK 锁失败 → 必须释放 Redis 锁 (回滚)
                //   学习要点: 加锁链路中任何一步失败都要回滚已获锁,避免脏锁
                _logger.LogWarning("双重锁: Redis 锁已获取但 ZK 锁失败, 回滚 Redis 锁, resource={Resource}", resource);
                await redisHandle.ReleaseAsync();
                PaymentMetrics.LockAcquireTotal.WithLabels("dual", "fail").Inc();
                PaymentMetrics.LockAcquireDurationSeconds.WithLabels("dual").Observe(sw.Elapsed.TotalSeconds);
                TraceContext.RecordError(new InvalidOperationException("ZK 锁获取失败"));
                return null;
            }
        }
        catch (Exception ex)
        {
            // 异常情况同样需要回滚
            _logger.LogError(ex, "双重锁: ZK 加锁异常, 回滚 Redis 锁, resource={Resource}", resource);
            await redisHandle.ReleaseAsync();
            throw;
        }

        _logger.LogInformation(
            "双重锁获取成功: resource={Resource}, redisLockId={RedisLockId}, zkLockId={ZkLockId}",
            resource, redisHandle.LockId, zkHandle.LockId);

        // ★ M6-3: 锁获取成功指标
        PaymentMetrics.LockAcquireTotal.WithLabels("dual", "success").Inc();
        PaymentMetrics.LockAcquireDurationSeconds.WithLabels("dual").Observe(sw.Elapsed.TotalSeconds);
        span?.SetTag("lock.redis_id", redisHandle.LockId);
        span?.SetTag("lock.zk_id", zkHandle.LockId);

        // 返回组合句柄,封装两个底层句柄的释放逻辑
        return new DualLockHandle(redisHandle, zkHandle, _logger, resource);
    }
}

// ============================================================================
// DualLockHandle —— 双重锁句柄,封装两个底层句柄
// ============================================================================
// 学习要点: 组合模式 (Composite Pattern)
//   业务层只看到一个 ILockHandle,内部组合 Redis + ZK 两个句柄
//   释放时按相反顺序 (ZK 先 → Redis 后)
// ============================================================================

internal class DualLockHandle : ILockHandle
{
    private readonly ILockHandle _redisHandle;
    private readonly ILockHandle _zkHandle;
    private readonly ILogger _logger;
    private int _released;

    public string Resource { get; }
    public string LockId => $"{_redisHandle.LockId}|{_zkHandle.LockId}";
    public int AcquiredNodeCount => _redisHandle.AcquiredNodeCount + _zkHandle.AcquiredNodeCount;

    public DualLockHandle(
        ILockHandle redisHandle,
        ILockHandle zkHandle,
        ILogger logger,
        string resource)
    {
        _redisHandle = redisHandle;
        _zkHandle = zkHandle;
        _logger = logger;
        Resource = resource;
    }

    public async Task<bool> ReleaseAsync()
    {
        if (Interlocked.Exchange(ref _released, 1) == 1)
            return true;

        // ★ 学习要点: 释放顺序与加锁顺序相反
        //   先释放 ZK (后加的先释放),此时 Redis 锁还在,其他实例进不来
        //   再释放 Redis (业务完全结束,允许下一个回调进入)
        var zkReleased = await _zkHandle.ReleaseAsync();
        var redisReleased = await _redisHandle.ReleaseAsync();

        _logger.LogInformation(
            "双重锁释放: resource={Resource}, zkReleased={Zk}, redisReleased={Redis}",
            Resource, zkReleased, redisReleased);

        return zkReleased || redisReleased;  // 任一成功即认为释放成功
    }

    /// <summary>续约 — 转发给两个底层句柄</summary>
    public async Task<bool> RenewAsync(TimeSpan extend)
    {
        // 学习要点: 续约同时续 Redis 和 ZK
        //   Redis 锁需要续约 (有 expiry),ZK 不需要 (会话保活),但调用无副作用
        var redisRenewed = await _redisHandle.RenewAsync(extend);
        var zkRenewed = await _zkHandle.RenewAsync(extend);
        return redisRenewed && zkRenewed;
    }

    public async ValueTask DisposeAsync()
    {
        await ReleaseAsync();
        await _redisHandle.DisposeAsync();
        await _zkHandle.DisposeAsync();
    }
}
