// ============================================================================
// 内存版 ZooKeeper 模拟实现
// ============================================================================
// 学习要点:
//   1. 用 Dictionary 模拟 ZNode 树(真实 ZK 也是内存数据库)
//   2. 实现四种节点类型 + Watch 触发机制
//   3. 临时节点会话失效时自动删除
//   4. 真实环境把此类替换为 ZooKeeperNetEx.ZooKeeper 即可
//
// 学习要点(共享存储):
//   - 多个 InMemoryZooKeeper 客户端实例共享同一份 ZNode 树
//   - 模拟真实 ZK 服务器:多个客户端连接同一个 ZK 服务
//   - 这样才能正确演示分布式锁 / Leader 选举等需要多客户端协作的场景
//
// 使用场景:
//   - 不启动 ZK 服务也能学习 API
//   - 单元测试快速验证逻辑
//   - 理解 ZK 内部实现原理
// ============================================================================

namespace ZooKeeperDemo;

public class InMemoryZooKeeper : IZooKeeperClient
{
    // 学习要点: 静态共享存储模拟真实 ZK 服务器
    //   - 所有 InMemoryZooKeeper 实例共享同一份 ZNode 树
    //   - 这样多个客户端才能看到彼此创建的节点(像真实 ZK)
    //   - 真实 ZK 也是这种模型:多个客户端连接同一个 ZK 服务器
    private static readonly Dictionary<string, ZNode> _sharedNodes = new();
    private static readonly Dictionary<string, List<Action<WatchEvent>>> _sharedWatchers = new();
    private static long _sharedSequenceCounter = 0;
    private static readonly object _sharedLock = new();

    static InMemoryZooKeeper()
    {
        // 初始化根节点(只初始化一次,所有客户端共享)
        _sharedNodes["/"] = new ZNode(
            Path: "/",
            Data: Array.Empty<byte>(),
            NodeType: ZNodeType.Persistent,
            Version: 0,
            EphemeralOwner: null,
            CreatedAt: DateTime.UtcNow,
            ModifiedAt: DateTime.UtcNow);
    }

    // 当前会话 ID(每个客户端实例独立,模拟不同会话)
    public long SessionId { get; } = Random.Shared.NextInt64(1000, 9999);
    public bool IsConnected { get; private set; } = true;

    public InMemoryZooKeeper()
    {
        // 学习要点: 不同客户端有不同 SessionId,但共享 ZNode 树
    }

    /// <summary>
    /// 创建节点
    /// </summary>
    public Task<string> CreateAsync(string path, byte[] data, ZNodeType type)
    {
        lock (_sharedLock)
        {
            // 校验:父节点必须存在
            var parentPath = GetParentPath(path);
            if (!_sharedNodes.ContainsKey(parentPath))
                throw new InvalidOperationException($"父节点不存在: {parentPath}");

            // 校验:节点不能已存在
            if (_sharedNodes.ContainsKey(path))
                throw new InvalidOperationException($"节点已存在: {path}");

            // 顺序节点: 追加单调递增序号
            var actualPath = path;
            if (type is ZNodeType.PersistentSequential or ZNodeType.EphemeralSequential)
            {
                var seq = Interlocked.Increment(ref _sharedSequenceCounter);
                actualPath = $"{path}{seq:D10}";   // 如 /lock/lock_0000000001
            }

            // 创建节点
            var node = new ZNode(
                Path: actualPath,
                Data: data,
                NodeType: type,
                Version: 0,
                EphemeralOwner: type is ZNodeType.Ephemeral or ZNodeType.EphemeralSequential
                    ? SessionId : null,
                CreatedAt: DateTime.UtcNow,
                ModifiedAt: DateTime.UtcNow);
            _sharedNodes[actualPath] = node;

            // 触发 Watch: 节点创建
            NotifyWatchers(actualPath, WatchEventType.NodeCreated);
            // 触发 Watch: 父节点子节点列表变化
            NotifyWatchers(parentPath, WatchEventType.NodeChildrenChanged);

            return Task.FromResult(actualPath);
        }
    }

    /// <summary>
    /// 读取节点数据
    /// </summary>
    public Task<ZNode?> GetDataAsync(string path, bool watch = false)
    {
        lock (_sharedLock)
        {
            if (!_sharedNodes.TryGetValue(path, out var node))
                return Task.FromResult<ZNode?>(null);

            if (watch)
                RegisterWatcher(path, _ => { });   // 注册空 Watch(真实 ZK 一次性触发)

            return Task.FromResult<ZNode?>(node);
        }
    }

    /// <summary>
    /// 获取子节点列表
    /// </summary>
    public Task<IEnumerable<string>> GetChildrenAsync(string path, bool watch = false)
    {
        lock (_sharedLock)
        {
            if (!_sharedNodes.ContainsKey(path))
                throw new InvalidOperationException($"节点不存在: {path}");

            var children = _sharedNodes.Keys
                .Where(k => k != path && k.StartsWith(path + "/"))
                .Select(k =>
                {
                    var rest = k.Substring(path.Length + 1);
                    return rest.Contains('/') ? rest.Substring(0, rest.IndexOf('/')) : rest;
                })
                .Distinct()
                .Select(c => path == "/" ? "/" + c : path + "/" + c)
                .ToList();

            if (watch)
                RegisterWatcher(path, _ => { });

            return Task.FromResult<IEnumerable<string>>(children);
        }
    }

    /// <summary>
    /// 检查节点是否存在
    /// </summary>
    public Task<bool> ExistsAsync(string path, bool watch = false)
    {
        lock (_sharedLock)
        {
            var exists = _sharedNodes.ContainsKey(path);
            if (watch && !exists)
                RegisterWatcher(path, _ => { });
            return Task.FromResult(exists);
        }
    }

    /// <summary>
    /// 修改节点数据(CAS 乐观锁)
    /// </summary>
    public Task SetDataAsync(string path, byte[] data, long expectedVersion = -1)
    {
        lock (_sharedLock)
        {
            if (!_sharedNodes.TryGetValue(path, out var node))
                throw new InvalidOperationException($"节点不存在: {path}");

            // CAS 版本校验
            if (expectedVersion != -1 && node.Version != expectedVersion)
                throw new InvalidOperationException($"版本不匹配: 期望 {expectedVersion}, 实际 {node.Version}");

            var updated = node with
            {
                Data = data,
                Version = node.Version + 1,
                ModifiedAt = DateTime.UtcNow
            };
            _sharedNodes[path] = updated;

            // 触发 Watch: 数据变化
            NotifyWatchers(path, WatchEventType.NodeDataChanged);

            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// 删除节点(CAS 乐观锁)
    /// </summary>
    public Task DeleteAsync(string path, long expectedVersion = -1)
    {
        lock (_sharedLock)
        {
            if (!_sharedNodes.TryGetValue(path, out var node))
                throw new InvalidOperationException($"节点不存在: {path}");

            if (expectedVersion != -1 && node.Version != expectedVersion)
                throw new InvalidOperationException($"版本不匹配");

            // 校验: 不能删除有子节点的节点
            if (_sharedNodes.Keys.Any(k => k.StartsWith(path + "/")))
                throw new InvalidOperationException($"节点有子节点,不能删除: {path}");

            _sharedNodes.Remove(path);

            // 触发 Watch: 节点删除
            NotifyWatchers(path, WatchEventType.NodeDeleted);
            // 父节点子节点列表变化
            NotifyWatchers(GetParentPath(path), WatchEventType.NodeChildrenChanged);

            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// 注册 Watch 回调
    /// </summary>
    public void RegisterWatcher(string path, Action<WatchEvent> callback)
    {
        lock (_sharedLock)
        {
            if (!_sharedWatchers.ContainsKey(path))
                _sharedWatchers[path] = new List<Action<WatchEvent>>();
            _sharedWatchers[path].Add(callback);
        }
    }

    /// <summary>
    /// 模拟会话失效(临时节点自动删除)
    /// 学习要点: 临时节点的核心特性,客户端断开会话自动删除
    ///   只删除当前会话(SessionId)拥有的临时节点,不影响其他会话
    /// </summary>
    public void SimulateSessionExpired()
    {
        IsConnected = false;
        List<string> ephemeralPaths;
        lock (_sharedLock)
        {
            ephemeralPaths = _sharedNodes
                .Where(kv => kv.Value.EphemeralOwner == SessionId)
                .Select(kv => kv.Key)
                .ToList();

            foreach (var path in ephemeralPaths)
            {
                _sharedNodes.Remove(path);
            }
        }

        // Watch 通知在锁外执行(避免回调中再次获取锁导致死锁)
        foreach (var path in ephemeralPaths)
        {
            NotifyWatchers(path, WatchEventType.NodeDeleted);
            NotifyWatchers(GetParentPath(path), WatchEventType.NodeChildrenChanged);
        }

        Console.WriteLine($"[ZK] 会话 {SessionId} 失效,自动删除 {ephemeralPaths.Count} 个临时节点");
    }

    private void NotifyWatchers(string path, WatchEventType eventType)
    {
        List<Action<WatchEvent>>? watchersCopy = null;
        lock (_sharedLock)
        {
            if (_sharedWatchers.TryGetValue(path, out var watchers))
                watchersCopy = watchers.ToList();
        }

        if (watchersCopy == null) return;

        var evt = new WatchEvent(eventType, path);
        foreach (var w in watchersCopy)
        {
            try { w(evt); }
            catch (Exception ex) { Console.WriteLine($"[Watch 错误] {ex.Message}"); }
        }
        // ZK 3.x 的 Watch 是一次性的,触发后自动失效
        // 学习用,这里不清理以便多次演示
    }

    private static string GetParentPath(string path)
    {
        if (path == "/") return "/";
        var idx = path.LastIndexOf('/');
        return idx <= 0 ? "/" : path.Substring(0, idx);
    }

    /// <summary>
    /// 重置共享存储(测试/演示用,清空所有节点)
    /// 学习要点: 真实 ZK 没有此 API,这里是 Demo 演示需要
    /// </summary>
    public static void ResetSharedStore()
    {
        lock (_sharedLock)
        {
            _sharedNodes.Clear();
            _sharedWatchers.Clear();
            _sharedSequenceCounter = 0;
            _sharedNodes["/"] = new ZNode(
                Path: "/",
                Data: Array.Empty<byte>(),
                NodeType: ZNodeType.Persistent,
                Version: 0,
                EphemeralOwner: null,
                CreatedAt: DateTime.UtcNow,
                ModifiedAt: DateTime.UtcNow);
        }
    }

    public void Dispose() => SimulateSessionExpired();
}
