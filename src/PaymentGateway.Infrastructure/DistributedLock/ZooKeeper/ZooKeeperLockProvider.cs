using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using PaymentGateway.Application.Abstractions;
using Zk = org.apache.zookeeper.ZooKeeper;  // 别名规避命名空间冲突

namespace PaymentGateway.Infrastructure.DistributedLock.ZooKeeper;

// ============================================================================
// ZooKeeperLockProvider —— 基于 ZK 临时顺序节点的分布式锁实现
// ============================================================================
// ★ 学习要点: ZK 分布式锁是"强一致锁"的代表,与 Redis Redlock 形成对比
//
// 【ZK 分布式锁算法 (羊群效应优化版)】
//   Step 1: 客户端连接 ZK,在锁根节点 /locks/payment-gateway 下创建
//           临时顺序节点 (EPHEMERAL_SEQUENTIAL),如 /locks/payment-gateway/order_123-00000001
//   Step 2: 客户端获取 /locks/payment-gateway 下所有子节点,排序
//   Step 3: 判断自己创建的节点是否是最小的:
//           - 是最小 → 加锁成功,执行业务
//           - 不是最小 → Watch(监视)比自己小一号的前驱节点
//   Step 4: 当前驱节点被删除时(持锁者释放),ZK 通知客户端 → 客户端再次检查
//           自己是否是最小,如是则获得锁
//
// 【为什么 ZK 比 Redis Redlock 更"强一致"?】
//   - ZK 是 CP 系统 (ZAB 协议保证一致性,半数以上节点写入才返回成功)
//   - Redis Cluster 是 AP 系统 (主从异步复制,故障切换期间可能丢锁)
//   - 临时节点 + 会话绑定: 客户端断连后 ZK 自动删除节点(防死锁)
//
// 【羊群效应 (Herd Effect) 与优化】
//   - 朴素实现: 所有客户端都 Watch 锁根节点 → 持锁者释放时,所有客户端被唤醒
//     浪费网络与 CPU (N 个客户端争抢 1 把锁)
//   - 优化方案: 只 Watch 前驱节点 → 持锁者释放时只有 1 个客户端被唤醒
//     (本实现采用此方案)
//
// 【临时顺序节点 EPHEMERAL_SEQUENTIAL 的两个关键属性】
//   - EPHEMERAL: 临时节点,客户端会话断开后自动删除 (防死锁)
//   - SEQUENTIAL: 节点名自动追加单调递增序号 (如 00000001, 00000002)
//     用于实现"FIFO 公平锁"——先到先得
// ============================================================================

public class ZooKeeperLockProvider : IDistributedLock, IAsyncDisposable
{
    private readonly LockOptions _options;
    private readonly ILogger<ZooKeeperLockProvider> _logger;
    private Zk? _zk;
    private readonly SemaphoreSlim _connectLock = new(1, 1);

    public ZooKeeperLockProvider(
        IOptions<LockOptions> options,
        ILogger<ZooKeeperLockProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 惰性连接 ZK (避免构造函数阻塞,首次加锁时才连接)
    /// </summary>
    private async Task<Zk> GetConnectedZkAsync(CancellationToken ct)
    {
        if (_zk != null && _zk.getState() == Zk.States.CONNECTED)
            return _zk;

        await _connectLock.WaitAsync(ct);
        try
        {
            if (_zk != null && _zk.getState() == Zk.States.CONNECTED)
                return _zk;

            // 学习要点: ZK 客户端连接是异步的,需要等待 Watcher 通知 Connected 事件
            var connectionTimeout = TimeSpan.FromMilliseconds(_options.ZkSessionTimeoutMs);
            var connectedEvent = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            // ★ ZooKeeperNetEx 构造函数 sessionTimeout 参数为 int (毫秒)
            //   学习要点: 注意不同 SDK 的 API 差异,这里需要 (int)
            var zk = new Zk(
                _options.ZooKeeperEndpoint,
                _options.ZkSessionTimeoutMs,
                new ConnectionWatcher(connectedEvent, _logger));

            // 等待连接建立 (最长等待 SessionTimeout)
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(connectionTimeout);
            using (cts.Token.Register(() => connectedEvent.TrySetCanceled(cts.Token)))
            {
                await connectedEvent.Task;
            }

            // ★ 学习要点: 确保锁根节点存在 (CreateMode.PERSISTENT 永久节点)
            //   - PERSISTENT: 客户端断开后不删除 (锁根节点必须保留)
            //   - EPHEMERAL: 临时节点 (子节点用,断开自动删)
            //   - 父节点不存在则创建子节点会抛 NoNodeException
            await EnsureRootNodeExistsAsync(zk, ct);

            _zk = zk;
            _logger.LogInformation("ZK 连接成功, 锁根节点: {Root}", _options.ZooKeeperLockRoot);
            return _zk;
        }
        finally
        {
            _connectLock.Release();
        }
    }

    /// <summary>确保锁根节点及其父节点都存在 (PERSISTENT 永久节点)</summary>
    /// <remarks>
    /// 学习要点: ZooKeeper 不支持递归创建节点
    ///   如根节点为 /locks/payment-gateway,必须先创建 /locks 再创建子节点
    ///   否则 createAsync("/locks/payment-gateway") 会抛 NoNodeException
    ///   (ZK 不会自动创建中间父节点,与文件系统不同)
    /// </remarks>
    private async Task EnsureRootNodeExistsAsync(Zk zk, CancellationToken ct)
    {
        // 逐级创建路径中的所有节点 (ZK 不支持自动创建父节点)
        var segments = _options.ZooKeeperLockRoot.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var currentPath = "";
        foreach (var segment in segments)
        {
            currentPath += "/" + segment;
            try
            {
                // 检查节点是否存在,不存在则创建 (防止并发创建抛 NodeExistsException)
                var stat = await zk.existsAsync(currentPath, false);
                if (stat == null)
                {
                    // CreateMode.PERSISTENT 永久节点,ACL 完全开放
                    //   ZooDefs.Ids.OPEN_ACL_UNSAFE = 任何人可读写创建
                    await zk.createAsync(
                        currentPath,
                        Array.Empty<byte>(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                    _logger.LogInformation("已创建 ZK 节点: {Path}", currentPath);
                }
            }
            catch (KeeperException.NodeExistsException)
            {
                // 并发创建时另一个实例先创建了,忽略即可
            }
        }
    }

    /// <summary>
    /// 尝试获取 ZK 分布式锁 (非阻塞语义,失败立即返回)
    /// </summary>
    /// <remarks>
    /// 学习要点: ZK 锁的"非阻塞"实现说明
    ///   - 严格 ZK 锁天然是阻塞的 (等待前驱节点释放)
    ///   - 本实现通过"立即判断是否最小节点"实现非阻塞:
    ///     如不是最小节点,直接返回失败 (用于 DualLock 场景,Redis 已先加锁,ZK 失败概率极低)
    ///   - 如需阻塞等待,可在调用方实现重试逻辑
    /// </remarks>
    public async Task<ILockHandle?> TryAcquireAsync(
        string resource,
        TimeSpan expiryTime,
        CancellationToken ct = default)
    {
        var zk = await GetConnectedZkAsync(ct);

        // ★ Step 1: 在锁根节点下创建临时顺序节点
        //   节点路径: {Root}/{resource}-{序号}
        //   - EPHEMERAL_SEQUENTIAL: 临时 + 顺序,序号自动追加
        //   - 节点 data 存 lockId (UUID) 供校验
        var lockId = Guid.NewGuid().ToString("N");
        var nodePath = $"{_options.ZooKeeperLockRoot}/{SanitizeResourceName(resource)}-";
        var nodeData = System.Text.Encoding.UTF8.GetBytes(lockId);

        string createdNodePath;
        try
        {
            // ★ 学习要点: createAsync 返回完整路径 (含自动追加的序号)
            //   如入参 /locks/payment-gateway/order_123-
            //   返回 /locks/payment-gateway/order_123-00000001
            createdNodePath = await zk.createAsync(
                nodePath,
                nodeData,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ZK 创建临时顺序节点失败: resource={Resource}", resource);
            return null;
        }

        // ★ Step 2: 获取所有子节点并排序
        //   学习要点: ZK 的 getChildren API
        //   - watch=false: 不监听 (这里只判断当前状态,后续才 Watch 前驱)
        //   - 子节点名形如 "order_123-00000001",按字符串排序即可 (ZK 序号是定长补零的)
        List<string> children;
        try
        {
            children = (await zk.getChildrenAsync(_options.ZooKeeperLockRoot, false)).Children;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ZK 获取子节点列表失败,清理已创建节点");
            await SafeDeleteNodeAsync(zk, createdNodePath);
            return null;
        }

        children.Sort(StringComparer.Ordinal);  // 字符串序号排序

        // 从完整路径中提取节点名 (getChildren 返回的是相对名称,不含父路径)
        var createdNodeName = createdNodePath.Substring(createdNodePath.LastIndexOf('/') + 1);
        var myIndex = children.IndexOf(createdNodeName);

        if (myIndex < 0)
        {
            // 异常情况: 创建的节点不在列表中(可能已被会话超时删除)
            _logger.LogError("ZK 锁异常: 创建的节点不在子节点列表中");
            return null;
        }

        // ★ Step 3: 判断是否最小节点
        if (myIndex == 0)
        {
            // 我是最小节点 → 加锁成功
            _logger.LogInformation(
                "ZK 加锁成功: resource={Resource}, lockId={LockId}, node={Node}",
                resource, lockId, createdNodeName);
            return new ZooKeeperLockHandle(
                zk, resource, lockId, createdNodePath, _logger);
        }

        // ★ Step 4: 不是最小 → Watch 前驱节点
        //   非阻塞语义: 直接返回失败 (DualLock 场景使用)
        //   阻塞语义: 这里应该 Watch 前驱节点,等待通知后再次检查
        //
        //   本实现为兼容 DualLockProvider 的"非阻塞"语义,直接返回失败
        //   如需阻塞 ZK 锁,可在此处实现 Watch + TaskCompletionSource 等待逻辑
        var predecessorNode = children[myIndex - 1];
        _logger.LogInformation(
            "ZK 加锁失败(已被持有): resource={Resource}, node={Node}, 前驱={Predecessor}",
            resource, createdNodeName, predecessorNode);

        // 清理已创建的节点 (避免脏节点残留)
        await SafeDeleteNodeAsync(zk, createdNodePath);
        return null;
    }

    /// <summary>安全删除节点 (失败不抛异常)</summary>
    private static async Task SafeDeleteNodeAsync(Zk zk, string path)
    {
        try
        {
            // ★ 学习要点: deleteAsync 第二参数 version
            //   -1 表示匹配任意版本 (不校验版本)
            //   正数表示校验节点版本,版本不匹配抛 BadVersionException (CAS 语义)
            await zk.deleteAsync(path, -1);
        }
        catch
        {
            // 忽略删除失败 (节点可能已被 ZK 因会话超时自动清理)
        }
    }

    /// <summary>对 resource 名做清洗 (ZK 节点名不允许特殊字符如 / : 等)</summary>
    private static string SanitizeResourceName(string resource)
    {
        // 学习要点: ZK 节点名不能包含 / , 且首字符不能是 -
        //   支付订单号一般是 "PG20250714001" 这种,直接用即可
        //   但为防御性编程,这里做清洗
        var sb = new System.Text.StringBuilder(resource.Length);
        foreach (var c in resource)
        {
            sb.Append(c == '/' || c == ':' || c == ' ' ? '_' : c);
        }
        return sb.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        if (_zk != null)
        {
            try
            {
                // ★ 学习要点: closeAsync 会触发会话结束,所有临时节点自动删除
                //   这是 ZK 锁"防死锁"的根本保障 (与 Redis 的 expiry 兜底不同)
                await _zk.closeAsync();
            }
            catch { /* 忽略关闭异常 */ }
        }
        _connectLock.Dispose();
    }
}

// ============================================================================
// ConnectionWatcher —— ZK 连接状态监听器
// ============================================================================
// 学习要点: ZK 的 Watcher 是一次性触发的 (One-time trigger)
//   - 连接建立后会触发 Watcher.Process 的 Event.KeeperState.SyncConnected
//   - 用 TaskCompletionSource 把"事件通知"转换为 awaitable Task
// ============================================================================

internal class ConnectionWatcher : Watcher
{
    private readonly TaskCompletionSource<bool> _tcs;
    private readonly ILogger _logger;

    public ConnectionWatcher(TaskCompletionSource<bool> tcs, ILogger logger)
    {
        _tcs = tcs;
        _logger = logger;
    }

    public override Task process(WatchedEvent @event)
    {
        var state = @event.getState();
        _logger.LogDebug("ZK 事件: state={State}, type={Type}, path={Path}", state, @event.get_Type(), @event.getPath());

        if (state == Event.KeeperState.SyncConnected ||
            state == Event.KeeperState.ConnectedReadOnly)
        {
            _tcs.TrySetResult(true);
        }
        else if (state == Event.KeeperState.Expired ||
                 state == Event.KeeperState.Disconnected)
        {
            _tcs.TrySetException(new InvalidOperationException($"ZK 连接失败: {state}"));
        }
        return Task.CompletedTask;
    }
}

// ============================================================================
// ZooKeeperLockHandle —— ZK 锁句柄实现
// ============================================================================
// 学习要点: ZK 锁的"释放"就是 delete 临时节点
//   - 删除节点后,后继节点的 Watcher 被触发,后继客户端被唤醒
//   - 不需要"续约" (ZK 会话保活由客户端后台心跳自动完成)
// ============================================================================

internal class ZooKeeperLockHandle : ILockHandle
{
    private readonly Zk _zk;
    private readonly ILogger _logger;
    private readonly string _nodePath;
    private int _released;  // 防止重复释放

    public string Resource { get; }
    public string LockId { get; }
    public int AcquiredNodeCount => 1;  // ZK 单节点语义,固定为 1

    public ZooKeeperLockHandle(
        Zk zk,
        string resource,
        string lockId,
        string nodePath,
        ILogger logger)
    {
        _zk = zk;
        Resource = resource;
        LockId = lockId;
        _nodePath = nodePath;
        _logger = logger;
    }

    public async Task<bool> ReleaseAsync()
    {
        // 学习要点: Interlocked 防止重复释放 (ZK delete 幂等性差,二次删除抛 NoNodeException)
        if (Interlocked.Exchange(ref _released, 1) == 1)
            return true;

        try
        {
            // ★ 释放锁 = delete 临时节点 (version=-1 不校验版本)
            //   删除后,后继节点的 Watcher 被触发,后继客户端收到通知后检查自己是否最小
            await _zk.deleteAsync(_nodePath, -1);
            _logger.LogInformation("ZK 锁释放: resource={Resource}, node={Node}", Resource, _nodePath);
            return true;
        }
        catch (KeeperException.NoNodeException)
        {
            // 节点已不存在 (可能 ZK 会话超时自动清理了)
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ZK 锁释放异常: resource={Resource}", Resource);
            return false;
        }
    }

    /// <summary>
    /// ZK 锁不需要手动续约 (会话保活由 ZK 客户端后台心跳完成)
    /// </summary>
    public Task<bool> RenewAsync(TimeSpan extend)
    {
        // 学习要点: ZK 与 Redis 锁的对比
        //   - Redis: 锁有过期时间,需看门狗续约
        //   - ZK: 临时节点绑定会话,会话心跳(每 SessionTimeout/3)自动保活
        //         只要客户端进程不退出,锁就不会因超时丢失
        //         客户端宕机则会话断开,临时节点自动删除 (防死锁)
        //   所以这里直接返回 true (No-op)
        return Task.FromResult(true);
    }

    public async ValueTask DisposeAsync()
    {
        await ReleaseAsync();
    }
}
