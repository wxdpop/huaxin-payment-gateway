using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PaymentGateway.Application.Abstractions;
using StackExchange.Redis;

namespace PaymentGateway.Infrastructure.DistributedLock.Redis;

// ============================================================================
// RedLockProvider —— Redlock 算法的 .NET 手动实现
// ============================================================================
// ★ 学习要点: 这是工程的核心实现之一,理解 Redlock 算法比直接用 RedLockNet 库更有价值
//
// 【Redlock 算法 5 步流程】(由 Redis 作者 antirez 提出):
//   Step 1: 获取当前时间 T1(毫秒)
//   Step 2: 向 N 个独立 Redis 实例并发发送 SET key value NX PX expiry
//           (本工程 N=6,quorum=N/2+1=4)
//   Step 3: 获取当前时间 T2,计算加锁耗时 = T2 - T1
//   Step 4: 加锁成功条件: 成功节点数 >= quorum 且 耗时 < 锁有效期
//           否则视为失败,向所有已获锁节点发送释放命令回滚
//   Step 5: 业务执行期间,看门狗后台任务周期续约(expiry/3),防业务超时
//
// 【为什么需要 N 个独立实例?】
//   - Redlock 论文要求 N 个完全独立的 Redis 实例(无主从关系)
//   - 每个实例单独加锁,多数派成功才算获锁,避免单点故障
//   - 独立实例无主从同步延迟,SET NX 立即生效,满足 Redlock 安全性假设
//
// 【★ 为什么不用 Redis Cluster?】
//   - Redis Cluster 在主从切换期间可能丢失 SET NX(主宕机,从未同步)
//     违背 Redlock 的安全性假设
//   - ★ Docker Desktop for Windows (WSL2) 网络限制:
//     Cluster 模式下,StackExchange.Redis 通过 CLUSTER NODES 发现的节点地址是
//     容器内部 IP(172.18.0.x),宿主机无法访问。cluster-announce-ip 无法同时
//     满足容器间和宿主机访问(127.0.0.1 容器内指向自己,hostname 不被 Redis 接受)
//   - 因此开发环境部署 6 个独立 Redis 实例,完全符合 Redlock 论文要求
//
// 【N 个独立 Multiplexer —— Redlock 论文标准实现】
//   - 每个实例用一个独立的 IConnectionMultiplexer 连接(只配一个 endpoint)
//   - 每个 SET 命令发到不同的 Redis 进程,真正实现"多节点独立投票"
//   - 无 CLUSTER NODES 拓扑发现,无 MOVED 重定向,连接简单可靠
//
// 【Lua 脚本保证释放/续约的原子性】
//   - 释放: if get(key) == lockId then del(key) end  ← 防止误删他人持有的锁
//   - 续约: if get(key) == lockId then pexpire(key, ms) end
//   - 学习要点: Redis 单线程模型下 Lua 脚本执行是原子的,不会被其他命令插入
// ============================================================================

public class RedLockProvider : IDistributedLock, IDisposable, IAsyncDisposable
{
    private readonly List<IConnectionMultiplexer> _connections;
    private readonly LockOptions _options;
    private readonly ILogger<RedLockProvider> _logger;
    private readonly int _nodeCount;

    // ★ 学习要点: 释放锁的 Lua 脚本(原子操作)
    //   1. GET key 拿到当前值
    //   2. 如果值等于 lockId(说明我还是持锁者),则 DEL
    //   3. 否则不动(锁已被他人抢占或已过期)
    private const string ReleaseScript = @"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end";

    // ★ 学习要点: 续约锁的 Lua 脚本(原子操作)
    //   1. GET key 校验持锁者
    //   2. 是我则 PEXPIRE 续约(毫秒),否则返回 0
    private const string RenewScript = @"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
else
    return 0
end";

    public RedLockProvider(
        IOptions<LockOptions> options,
        ILogger<RedLockProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
        _nodeCount = _options.RedisEndpoints.Count;

        // ★ 为每个 Redis 实例创建独立的 Multiplexer(每个只连一个 endpoint)
        //   这是 Redlock 论文的标准实现:N 个独立连接 → N 次独立加锁 → 多数派投票
        //   每个 Multiplexer 不涉及集群拓扑发现,连接简单可靠
        _connections = new List<IConnectionMultiplexer>(_nodeCount);
        foreach (var endpoint in _options.RedisEndpoints)
        {
            var config = new ConfigurationOptions
            {
                AllowAdmin = true,
                // ★ 学习要点: StackExchange.Redis 同步操作超时(单位 ms)
                //   过小(如 1s): 高负载时易 TimeoutException 误判加锁失败
                //   过大(如 15s): Redis 不可达时拖累加锁流程
                //   5s 兼顾性能与容错,适合独立实例直连场景
                SyncTimeout = 5000,
                ConnectRetry = 3,
                ConnectTimeout = 5000,
                AbortOnConnectFail = false
            };
            config.EndPoints.Add(endpoint);
            _connections.Add(ConnectionMultiplexer.Connect(config));
        }

        _logger.LogInformation("RedLockProvider 初始化完成,节点数={Nodes}, Quorum={Q}",
            _nodeCount, _options.QuorumCount);
    }

    /// <summary>
    /// 尝试获取 Redlock 分布式锁(非阻塞,失败立即返回)
    /// </summary>
    public async Task<ILockHandle?> TryAcquireAsync(
        string resource,
        TimeSpan expiryTime,
        CancellationToken ct = default)
    {
        var lockId = Guid.NewGuid().ToString("N");
        var expiryMs = (int)expiryTime.TotalMilliseconds;

        // ★ Step 1: 记录起始时间(用于计算加锁耗时,见 Step 3)
        var sw = Stopwatch.StartNew();

        // ★ Step 2: 并发向所有实例发送 SET NX PX
        //   关键: 每个实例用独立的 Multiplexer,命令发到不同的 Redis 进程
        //   独立实例无 slot/路由概念,所有实例用同一个 key,互不干扰
        var databases = _connections.Select(c => c.GetDatabase()).ToArray();
        var tasks = Enumerable.Range(0, _nodeCount).Select(idx =>
            TryLockOnNodeAsync(databases[idx], resource, lockId, expiryMs, ct));
        var results = await Task.WhenAll(tasks);

        var successCount = results.Count(r => r);
        sw.Stop();

        // ★ Step 3 & 4: 多数派判断 + 耗时校验
        //   学习要点: 双重校验 —— 既要有 quorum 个成功,也要校验"剩余有效期"
        //     如果加锁耗时接近或超过 expiryTime,即使 quorum 成功也视为失败
        //     原因: 锁可能即将过期,此时获取的锁无意义(业务还没开始执行锁就到期了)
        var elapsedMs = sw.ElapsedMilliseconds;
        var remainingMs = expiryMs - elapsedMs;

        if (successCount >= _options.QuorumCount && remainingMs > 0)
        {
            _logger.LogInformation(
                "RedLock 加锁成功: resource={Resource}, lockId={LockId}, 成功={Success}/{Total}, 耗时={Elapsed}ms, 剩余={Remaining}ms",
                resource, lockId, successCount, _nodeCount, elapsedMs, remainingMs);

            var handle = new RedLockHandle(
                resource, lockId, databases, _nodeCount, successCount, _options, _logger, expiryMs);

            // ★ Step 5: 启动看门狗续约(后台任务)
            //   看门狗负责: 业务执行时间超过 expiry 时,周期续约防锁过期
            //   仅当 expiryTime 较短(如 5s)且业务可能耗时长时启用
            //   对于"回调幂等"等短业务可不启用看门狗,本工程统一启用便于学习
            handle.StartWatchdog();

            return handle;
        }

        // ★ 加锁失败: 回滚已获锁节点
        //   学习要点: 即使最终判定失败,部分节点可能已 SET 成功,必须释放避免脏锁
        //   并发向所有节点发送释放脚本(无论是否持锁,失败也无所谓)
        _logger.LogWarning(
            "RedLock 加锁失败: resource={Resource}, 成功={Success}/{Total}, Quorum={Q}, 耗时={Elapsed}ms",
            resource, successCount, _nodeCount, _options.QuorumCount, elapsedMs);

        _ = ReleaseAcrossNodesAsync(databases, resource, lockId);
        return null;
    }

    /// <summary>
    /// 生成锁 key —— 所有节点用同一个 key(独立实例无需 Hash Tag)
    /// 学习要点:
    ///   - 独立 Redis 实例没有 slot 概念,直接用同一 key 即可
    ///   - 每个 SET 命令发到不同实例,互不干扰,天然实现"多节点独立投票"
    /// </summary>
    private static string GetNodeKey(string resource)
    {
        return $"redlock:{resource}";
    }

    /// <summary>对单个 Redis 实例尝试加锁</summary>
    private static async Task<bool> TryLockOnNodeAsync(
        IDatabase db,
        string resource,
        string lockId,
        int expiryMs,
        CancellationToken ct)
    {
        try
        {
            var key = GetNodeKey(resource);
            // ★ SET key value NX PX ms  (NX=不存在才设置, PX=毫秒过期)
            //   返回 true 表示加锁成功(之前不存在), false 表示已被其他客户端持有
            //   学习要点: StringSetAsync 第4参数 when = When.NotExists 实现 NX 语义
            var success = await db.StringSetAsync(
                key: key,
                value: lockId,
                expiry: TimeSpan.FromMilliseconds(expiryMs),
                when: When.NotExists,
                flags: CommandFlags.None);
            return success;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // 单节点失败不影响其他节点(Redlock 容错核心)
            //   学习要点: 网络分区时部分节点不可达,只要多数派可达即可工作
            return false;
        }
    }

    /// <summary>并发向所有实例释放锁(Lua 脚本保证原子性)</summary>
    private static async Task ReleaseAcrossNodesAsync(
        IDatabase[] databases, string resource, string lockId)
    {
        var key = GetNodeKey(resource);
        var tasks = databases.Select(db =>
        {
            try
            {
                // ★ 学习要点: 用 Lua 脚本而非先 GET 后 DEL
                //   GET + DEL 是两条命令,中间可能被其他客户端操作(竞态)
                //   Lua 脚本在 Redis 单线程中是原子的,执行期间不会被其他命令插入
                return db.ScriptEvaluateAsync(
                    ReleaseScript,
                    keys: new RedisKey[] { key },
                    values: new RedisValue[] { lockId });
            }
            catch
            {
                // 释放失败不影响业务,锁有过期时间兜底自动失效
                return Task.FromResult(RedisResult.Create((RedisValue)0));
            }
        });
        await Task.WhenAll(tasks);
    }

    public void Dispose()
    {
        foreach (var conn in _connections)
        {
            try { conn.Dispose(); } catch { /* 忽略 */ }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var conn in _connections)
        {
            try { await conn.DisposeAsync(); } catch { /* 忽略 */ }
        }
    }
}

// ============================================================================
// RedLockHandle —— Redlock 锁句柄实现
// ============================================================================
// 职责:
//   1. 持有 resource + lockId + 各实例 IDatabase,供释放/续约时使用
//   2. 后台看门狗周期续约,防止业务执行时间超过锁过期
//   3. 实现 IAsyncDisposable: 支持 await using 自动释放
// ============================================================================

internal class RedLockHandle : ILockHandle
{
    private readonly IDatabase[] _databases;
    private readonly int _nodeCount;
    private readonly LockOptions _options;
    private readonly ILogger _logger;
    private readonly int _expiryMs;

    // ★ 看门狗续约使用的 CancellationTokenSource
    //   释放锁时通过 Cancel 通知看门狗退出
    private CancellationTokenSource? _watchdogCts;
    private Task? _watchdogTask;

    // 释放锁的 Lua 脚本(同 RedLockProvider)
    private const string ReleaseScript = @"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end";

    private const string RenewScript = @"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
else
    return 0
end";

    public string Resource { get; }
    public string LockId { get; }
    public int AcquiredNodeCount { get; }

    public RedLockHandle(
        string resource,
        string lockId,
        IDatabase[] databases,
        int nodeCount,
        int acquiredNodeCount,
        LockOptions options,
        ILogger logger,
        int expiryMs)
    {
        Resource = resource;
        LockId = lockId;
        _databases = databases;
        _nodeCount = nodeCount;
        AcquiredNodeCount = acquiredNodeCount;
        _options = options;
        _logger = logger;
        _expiryMs = expiryMs;
    }

    /// <summary>
    /// 生成锁 key(同 RedLockProvider.GetNodeKey)
    /// </summary>
    private static string GetNodeKey(string resource)
    {
        return $"redlock:{resource}";
    }

    /// <summary>
    /// 启动看门狗续约任务
    /// </summary>
    public void StartWatchdog()
    {
        // ★ 学习要点: 看门狗机制(参考 Redisson 的 Java 实现)
        //   原理: 后台任务每 expiry/3 周期向所有节点发送续约脚本
        //   何时退出: 业务调用 ReleaseAsync 时通过 cts.Cancel() 通知
        //
        //   取 expiry/3 的理由:
        //     - 太频繁(如 expiry/10): 浪费网络,且 Lua 脚本阻塞 Redis
        //     - 太稀疏(如 expiry/2): 锁可能正好过期才续约,风险高
        //     - expiry/3 是业界惯例: 给业务留出 2/3 时间,1/3 时点续约

        _watchdogCts = new CancellationTokenSource();
        var renewIntervalMs = (int)(_expiryMs / _options.WatchdogRenewRatio);

        _watchdogTask = Task.Run(async () =>
        {
            try
            {
                while (!_watchdogCts.Token.IsCancellationRequested)
                {
                    // 等待续约周期(可被 Cancel 打断)
                    await Task.Delay(renewIntervalMs, _watchdogCts.Token);

                    if (_watchdogCts.Token.IsCancellationRequested)
                        break;

                    // 向所有节点并发发送续约脚本
                    var renewed = await RenewAcrossNodesAsync();

                    // ★ 多数派续约失败则认为锁已失效
                    //   学习要点: 续约也要多数派成功才算有效
                    //   少数节点续约成功无意义(其他节点已过期,可能被他人加锁)
                    if (renewed < _options.QuorumCount)
                    {
                        _logger.LogError(
                            "看门狗续约失败: resource={Resource}, 成功={Success}/{Total}, 锁可能已失效",
                            Resource, renewed, _nodeCount);
                        break;  // 锁失效,看门狗退出
                    }

                    _logger.LogDebug("看门狗续约成功: resource={Resource}, 续约数={Renewed}", Resource, renewed);
                }
            }
            catch (OperationCanceledException)
            {
                // 正常退出(锁已释放)
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "看门狗任务异常: resource={Resource}", Resource);
            }
        }, _watchdogCts.Token);
    }

    /// <summary>
    /// 释放锁 —— 向所有实例发送 Lua 释放脚本,并停止看门狗
    /// </summary>
    public async Task<bool> ReleaseAsync()
    {
        // 1. 停止看门狗续约(避免释放后续约又续上)
        _watchdogCts?.Cancel();
        try { if (_watchdogTask != null) await _watchdogTask; } catch { /* 忽略取消异常 */ }

        // 2. 并发向所有节点发送释放脚本(Lua 保证原子性)
        var successCount = await ReleaseAcrossNodesAsync();
        _logger.LogInformation(
            "RedLock 释放: resource={Resource}, lockId={LockId}, 成功释放={Success}/{Total}",
            Resource, LockId, successCount, _nodeCount);

        // 多数派释放成功即认为释放成功
        return successCount >= _options.QuorumCount;
    }

    /// <summary>手动续约(供外部业务在长时间操作时主动续约)</summary>
    public async Task<bool> RenewAsync(TimeSpan extend)
    {
        var extendMs = (int)extend.TotalMilliseconds;
        var key = GetNodeKey(Resource);
        var tasks = _databases.Select(async db =>
        {
            try
            {
                var result = (RedisResult?)await db.ScriptEvaluateAsync(
                    RenewScript,
                    keys: new RedisKey[] { key },
                    values: new RedisValue[] { LockId, extendMs });
                return result?.ToString() == "1";
            }
            catch { return false; }
        });
        var results = await Task.WhenAll(tasks);
        var renewed = results.Count(r => r);
        return renewed >= _options.QuorumCount;
    }

    /// <summary>并发向所有实例发送 Lua 释放脚本,返回成功数</summary>
    private async Task<int> ReleaseAcrossNodesAsync()
    {
        var key = GetNodeKey(Resource);
        var tasks = _databases.Select(async db =>
        {
            try
            {
                var result = (RedisResult?)await db.ScriptEvaluateAsync(
                    ReleaseScript,
                    keys: new RedisKey[] { key },
                    values: new RedisValue[] { LockId });
                return result?.ToString() == "1" ? 1 : 0;
            }
            catch { return 0; }
        });
        var results = await Task.WhenAll(tasks);
        return results.Sum();
    }

    /// <summary>并发向所有实例发送 Lua 续约脚本,返回成功数</summary>
    private async Task<int> RenewAcrossNodesAsync()
    {
        var key = GetNodeKey(Resource);
        var tasks = _databases.Select(async db =>
        {
            try
            {
                var result = (RedisResult?)await db.ScriptEvaluateAsync(
                    RenewScript,
                    keys: new RedisKey[] { key },
                    values: new RedisValue[] { LockId, _expiryMs });
                return result?.ToString() == "1" ? 1 : 0;
            }
            catch { return 0; }
        });
        var results = await Task.WhenAll(tasks);
        return results.Sum();
    }

    public async ValueTask DisposeAsync()
    {
        await ReleaseAsync();
        _watchdogCts?.Dispose();
    }
}
