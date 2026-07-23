// ============================================================================
// 真实 ZooKeeper 客户端适配器
// ============================================================================
// 学习要点:
//   1. 适配器模式:把 ZooKeeperNetEx 库适配到 IZooKeeperClient 接口
//   2. ZooKeeperNetEx 是 Java ZK 客户端的 .NET 移植版,API 风格接近 Java:
//      - 命名空间 org.apache.zookeeper(不是 ZooKeeperNet)
//      - Watcher 是抽象类(不是接口),方法名 process(WatchedEvent)
//      - 异步 API:createAsync/getDataAsync/...(没有同步版本)
//      - 返回值用 DataResult/ChildrenResult 包装(含 Data + Stat 字段)
//      - Stat 字段是小写:version/ephemeralOwner/ctime/mtime
//      - getter 方法是 Java 风格:getSessionId()/getState()/getPath()
//   3. Watch 是一次性的,触发后需要重新注册(ZK 3.x 协议限制)
//
// 使用前提:
//   1. Docker 启动 ZK: docker run -d --name zk -p 2181:2181 confluentinc/cp-zookeeper
//   2. NuGet: dotnet add package ZooKeeperNetEx
//
// 启动:
//   dotnet run --project examples/ZooKeeperDemo -- real
// ============================================================================

using org.apache.zookeeper;
using org.apache.zookeeper.data;
using ZooKeeperNetExZooKeeper = org.apache.zookeeper.ZooKeeper;
using ZooKeeperEvent = org.apache.zookeeper.Watcher.Event;
using StatType = org.apache.zookeeper.data.Stat;

namespace ZooKeeperDemo;

/// <summary>
/// 真实 ZooKeeper 客户端(适配 ZooKeeperNetEx 3.4.12.4 到 IZooKeeperClient)
/// </summary>
public class RealZooKeeperClient : IZooKeeperClient
{
    private readonly ZooKeeperNetExZooKeeper _zk;
    private readonly Dictionary<string, List<Action<WatchEvent>>> _watchers = new();
    private readonly object _watcherLock = new();

    // ZooKeeperNetEx 的 ZooKeeper 不实现 IDisposable,通过 closeAsync 关闭
    public long SessionId => _zk.getSessionId();
    public bool IsConnected => _zk.getState() == ZooKeeperNetExZooKeeper.States.CONNECTED;

    /// <summary>
    /// 连接 ZK
    /// </summary>
    /// <param name="connectString">ZK 地址,如 "127.0.0.1:2181"</param>
    /// <param name="sessionTimeoutSeconds">会话超时(秒,ZK 协议实际按毫秒传)</param>
    public RealZooKeeperClient(string connectString = "127.0.0.1:2181", int sessionTimeoutSeconds = 30)
    {
        Console.WriteLine($"[ZK] 正在连接 {connectString}...");

        // ZooKeeperNetEx 构造参数: connectString, sessionTimeout(ms), watcher, canBeReadOnly
        var sessionTimeoutMs = (int)TimeSpan.FromSeconds(sessionTimeoutSeconds).TotalMilliseconds;
        _zk = new ZooKeeperNetExZooKeeper(connectString, sessionTimeoutMs, new ConnectionWatcher(this), false);

        // 等待连接建立(ZK 是异步事件驱动,构造函数返回时未真正连上)
        WaitForConnection(TimeSpan.FromSeconds(10)).GetAwaiter().GetResult();
        Console.WriteLine($"[ZK] 已连接,SessionId: {_zk.getSessionId()}");
    }

    public async Task<string> CreateAsync(string path, byte[] data, ZNodeType type)
    {
        var zkMode = type switch
        {
            ZNodeType.Persistent            => CreateMode.PERSISTENT,
            ZNodeType.PersistentSequential  => CreateMode.PERSISTENT_SEQUENTIAL,
            ZNodeType.Ephemeral             => CreateMode.EPHEMERAL,
            ZNodeType.EphemeralSequential   => CreateMode.EPHEMERAL_SEQUENTIAL,
            _ => CreateMode.PERSISTENT
        };

        // createAsync: Task<string> createAsync(path, data, List<ACL>, CreateMode)
        // OPEN_ACL_UNSAFE 是 ZK 内置 ACL,允许任何人读写(测试用)
        var createdPath = await _zk.createAsync(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, zkMode);
        return createdPath;
    }

    public async Task<ZNode?> GetDataAsync(string path, bool watch = false)
    {
        // getDataAsync 返回 DataResult(含 Data + Stat 两个字段)
        DataResult? result;
        try
        {
            result = await _zk.getDataAsync(path, watch);
        }
        catch (KeeperException.NoNodeException)
        {
            return null;
        }

        if (result == null) return null;

        var stat = result.Stat;
        // 学习要点: Stat 字段是非 public,需通过 Java 风格 getter 访问
        //   getVersion() / getEphemeralOwner() / getCtime() / getMtime()
        // ephemeralOwner != 0 表示临时节点
        var ephemeralOwner = stat.getEphemeralOwner();
        var nodeType = ephemeralOwner != 0 ? ZNodeType.Ephemeral : ZNodeType.Persistent;

        return new ZNode(
            Path: path,
            Data: result.Data ?? Array.Empty<byte>(),
            NodeType: nodeType,
            Version: stat.getVersion(),
            EphemeralOwner: ephemeralOwner != 0 ? ephemeralOwner : null,
            // getCtime()/getMtime() 返回 Unix 毫秒时间戳
            CreatedAt: DateTimeOffset.FromUnixTimeMilliseconds(stat.getCtime()).UtcDateTime,
            ModifiedAt: DateTimeOffset.FromUnixTimeMilliseconds(stat.getMtime()).UtcDateTime);
    }

    public async Task<IEnumerable<string>> GetChildrenAsync(string path, bool watch = false)
    {
        try
        {
            // getChildrenAsync 返回 ChildrenResult(含 Children: List<string> + Stat)
            // 学习要点: 真实 ZK 的 getChildren 只返回子节点名称(不含父路径)
            //   如 path="/locks/order",返回 ["lock_0000000001", "lock_0000000002"]
            //   而 InMemory 模式返回完整路径 ["/locks/order/lock_0000000001"]
            //   为保持接口一致性,这里统一拼成完整路径
            var result = await _zk.getChildrenAsync(path, watch);
            var children = result.Children ?? new List<string>();
            var normalizedPath = path == "/" ? "" : path;
            return children.Select(c => $"{normalizedPath}/{c}").ToList();
        }
        catch (KeeperException.NoNodeException)
        {
            return new List<string>();
        }
    }

    public async Task<bool> ExistsAsync(string path, bool watch = false)
    {
        // existsAsync 返回 Task<Stat>,节点不存在时返回 null
        var stat = await _zk.existsAsync(path, watch);
        return stat != null;
    }

    public async Task SetDataAsync(string path, byte[] data, long expectedVersion = -1)
    {
        // setDataAsync 返回 Task<Stat>,版本不匹配抛 KeeperException.BadVersionException
        await _zk.setDataAsync(path, data, (int)expectedVersion);
    }

    public async Task DeleteAsync(string path, long expectedVersion = -1)
    {
        try
        {
            await _zk.deleteAsync(path, (int)expectedVersion);
        }
        catch (KeeperException.NoNodeException)
        {
            // 节点不存在视为已删除
        }
    }

    /// <summary>
    /// 注册 Watch(真实 ZK 3.x 的 Watch 是一次性的,触发后自动失效)
    /// 学习要点: ZK 3.x Watch 的核心特性
    ///   - Watch 一次性:触发一次后失效,需重新注册才能继续监听
    ///   - 触发顺序:服务端事件先于回调执行返回(保证客户端看到最新状态)
    ///   - ZK 3.6+ 支持 addWatch(永久 Watch),本 Demo 用 3.4 协议不支持
    /// </summary>
    public void RegisterWatcher(string path, Action<WatchEvent> callback)
    {
        lock (_watcherLock)
        {
            if (!_watchers.ContainsKey(path))
                _watchers[path] = new List<Action<WatchEvent>>();
            _watchers[path].Add(callback);
        }

        // 真实 ZK 必须通过 getData/exists/getChildren 调用注册 Watch
        // 学习要点: 用 ConfigureAwait(false) + fire-and-forget 避免同步等待死锁
        //   ZK 客户端的 Watch 回调在 IO 线程触发,同步等待会导致死锁
        _ = RegisterWatcherAsync(path);
    }

    private async Task RegisterWatcherAsync(string path)
    {
        try
        {
            await _zk.existsAsync(path, new InternalWatcher(path, OnWatchEvent)).ConfigureAwait(false);
        }
        catch
        {
            // 节点不存在时 existsAsync 会注册 NodeCreated Watch
        }
    }

    private void OnWatchEvent(WatchEvent evt)
    {
        List<Action<WatchEvent>>? callbacks;
        lock (_watcherLock)
        {
            if (!_watchers.TryGetValue(evt.Path, out callbacks)) return;
            callbacks = callbacks.ToList();
            // ZK 3.x Watch 一次性,触发后清除本批回调
            _watchers[evt.Path].Clear();
        }
        foreach (var cb in callbacks)
        {
            try { cb(evt); }
            catch (Exception ex) { Console.WriteLine($"[Watch 错误] {ex.Message}"); }
        }

        // 自动重新注册 Watch(模拟永久 Watch 效果)
        // 学习要点: 实际项目用 Curator 的 Cache 机制自动重注册,避免重复代码
        //   这里用 fire-and-forget 避免阻塞 ZK 事件线程
        _ = RegisterWatcherAsync(evt.Path);
    }

    private async Task WaitForConnection(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline && !IsConnected)
            await Task.Delay(100);
        if (!IsConnected)
            throw new TimeoutException($"ZK 连接超时({timeout.TotalSeconds}s),请检查 ZK 服务是否启动");
    }

    public void Dispose()
    {
        try { _zk.closeAsync().GetAwaiter().GetResult(); }
        catch { /* 忽略关闭异常 */ }
    }

    // ============================================================
    // 内部 Watcher 实现
    // 学习要点: ZooKeeperNetEx 的 Watcher 是抽象类(非接口)
    //   - 继承 Watcher 并实现 Task process(WatchedEvent)
    //   - 方法名是小写 process(Java 风格)
    // ============================================================

    private class ConnectionWatcher : Watcher
    {
        private readonly RealZooKeeperClient _client;
        public ConnectionWatcher(RealZooKeeperClient client) => _client = client;

        public override Task process(WatchedEvent @event)
        {
            // 连接级事件:状态变化(SyncConnected/Disconnected/Expired)
            Console.WriteLine($"  [ZK Event] {@event.getState()} - {@event.get_Type()} - {@event.getPath()}");
            return Task.CompletedTask;
        }
    }

    private class InternalWatcher : Watcher
    {
        private readonly string _path;
        private readonly Action<WatchEvent> _callback;

        public InternalWatcher(string path, Action<WatchEvent> callback)
        {
            _path = path;
            _callback = callback;
        }

        public override Task process(WatchedEvent @event)
        {
            // Watcher.Event.EventType: NodeCreated/NodeDeleted/NodeDataChanged/NodeChildrenChanged/None
            var type = @event.get_Type() switch
            {
                ZooKeeperEvent.EventType.NodeCreated         => WatchEventType.NodeCreated,
                ZooKeeperEvent.EventType.NodeDeleted         => WatchEventType.NodeDeleted,
                ZooKeeperEvent.EventType.NodeDataChanged    => WatchEventType.NodeDataChanged,
                ZooKeeperEvent.EventType.NodeChildrenChanged => WatchEventType.NodeChildrenChanged,
                _ => WatchEventType.NodeDataChanged
            };
            _callback(new WatchEvent(type, _path));
            return Task.CompletedTask;
        }
    }
}
