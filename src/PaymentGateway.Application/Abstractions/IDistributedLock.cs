using System.ComponentModel;

namespace PaymentGateway.Application.Abstractions;

// ============================================================================
// 分布式锁统一抽象 (IDistributedLock + ILockHandle + LockOptions)
// ============================================================================
// ★ 学习要点: DDD 依赖倒置原则
//   - 接口定义在 Application 层(业务需要的能力)
//   - 实现在 Infrastructure 层(RedLockProvider / ZooKeeperLockProvider)
//   - 业务依赖抽象,不感知具体实现
//
// 将此抽象放在 Application 层而非 Infrastructure 层的原因:
//   - Application 层的 HandleCallbackService 需要使用分布式锁
//   - 如果接口在 Infrastructure,Application 需引用 Infrastructure → 循环依赖
//   - 正确做法: Application 定义接口,Infrastructure 实现(依赖倒置)
// ============================================================================

public interface IDistributedLock
{
    Task<ILockHandle?> TryAcquireAsync(
        string resource,
        TimeSpan expiryTime,
        CancellationToken ct = default);
}

public interface ILockHandle : IAsyncDisposable
{
    string Resource { get; }
    string LockId { get; }
    int AcquiredNodeCount { get; }
    Task<bool> ReleaseAsync();
    Task<bool> RenewAsync(TimeSpan extend);
}

public class LockOptions
{
    /// <summary>Redlock 投票节点列表 (N 个独立 Redis 实例,论文要求完全独立无主从)</summary>
    public List<string> RedisEndpoints { get; set; } = new();

    /// <summary>ZooKeeper 连接地址 (如 localhost:2181)</summary>
    public string ZooKeeperEndpoint { get; set; } = string.Empty;

    /// <summary>ZK 锁根节点路径 (所有锁节点创建在此路径下,如 /locks/payment-gateway)</summary>
    public string ZooKeeperLockRoot { get; set; } = "/locks/payment-gateway";

    /// <summary>ZK 会话超时(毫秒,默认 30s) — 超时未心跳则临时节点自动删除(锁释放)</summary>
    public int ZkSessionTimeoutMs { get; set; } = 30_000;

    /// <summary>锁默认有效期(毫秒,默认 5s) — 业务应在此时间内完成,否则需看门狗续约</summary>
    public int DefaultExpiryMs { get; set; } = 5_000;

    /// <summary>看门狗续约比例(默认 3.0,即每 expiry/3 续约一次)</summary>
    public double WatchdogRenewRatio { get; set; } = 3.0;

    /// <summary>加锁尝试超时(毫秒,默认 200ms) — 单次 TryAcquire 等待上限</summary>
    public int AcquireTimeoutMs { get; set; } = 200;

    /// <summary>加锁失败重试次数(默认 3 次)</summary>
    public int RetryCount { get; set; } = 3;

    /// <summary>重试基础间隔(毫秒,默认 100ms) — 实际间隔 = base * 2^(attempt-1) 指数退避</summary>
    public int RetryBaseDelayMs { get; set; } = 100;

    /// <summary>多数派阈值(= N/2+1) — Redlock 成功所需最少节点数,自动计算</summary>
    public int QuorumCount => (RedisEndpoints.Count / 2) + 1;
}

public enum DistributedLockType
{
    [Description("Redis Redlock 算法: 高性能,适合短时高频加锁(支付回调幂等)")]
    Redlock,
    [Description("ZooKeeper 强一致锁: CP 系统,适合资金账户变更等强一致场景")]
    ZooKeeper,
    [Description("双重锁(Redis 快锁 + ZK 强一致锁): 兼顾性能与一致性,资金安全最佳实践")]
    Dual
}

/// <summary>锁获取结果状态 (供调用方决定后续动作)</summary>
public enum LockAcquireStatus
{
    /// <summary>成功获取锁</summary>
    Acquired,

    /// <summary>锁竞争(已被其他客户端持有) — 可重试或快速失败</summary>
    Contended,

    /// <summary>可重试的失败(网络抖动、多数派未达成但节点可达) — 建议指数退避重试</summary>
    RetryableFailure,

    /// <summary>致命错误(配置错误、所有节点不可达) — 不应重试,需人工介入</summary>
    FatalError
}
