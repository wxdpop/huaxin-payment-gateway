// ============================================================================
// 内存版 ZooKeeper 模拟实现
// ============================================================================
// 学习要点:
//   1. 用 Dictionary 模拟 ZNode 树(真实 ZK 也是内存数据库)
//   2. 实现四种节点类型 + Watch 触发机制
//   3. 临时节点会话失效时自动删除
//   4. 真实环境把此类替换为 ZooKeeperNetEx.ZooKeeper 即可
//
// 使用场景:
//   - 不启动 ZK 服务也能学习 API
//   - 单元测试快速验证逻辑
//   - 理解 ZK 内部实现原理
// ============================================================================

namespace ZooKeeperDemo;

public class InMemoryZooKeeper : IZooKeeperClient
{
    // ZNode 树(用字典模拟,Key 是路径)
    private readonly Dictionary<string, ZNode> _nodes = new();
    // Watch 回调注册表
    private readonly Dictionary<string, List<Action<WatchEvent>>> _watchers = new();
    // 顺序节点序号(模拟 ZK 的 zxid 单调递增)
    private long _sequenceCounter = 0;
    // 当前会话 ID(模拟 ZK 会话)
    public long SessionId { get; } = Random.Shared.NextInt64(1000, 9999);
    public bool IsConnected { get; private set; } = true;

    public InMemoryZooKeeper()
    {
        // 初始化根节点
        _nodes["/"] = new ZNode(
            Path: "/",
            Data: Array.Empty<byte>(),
            NodeType: ZNodeType.Persistent,
            Version: 0,
            EphemeralOwner: null,
            CreatedAt: DateTime.UtcNow,
            ModifiedAt: DateTime.UtcNow);
    }

    /// <summary>
    /// 创建节点
    /// </summary>
    public Task<string> CreateAsync(string path, byte[] data, ZNodeType type)
    {
        // 校验:父节点必须存在
        var parentPath = GetParentPath(path);
        if (!_nodes.ContainsKey(parentPath))
            throw new InvalidOperationException($"父节点不存在: {parentPath}");

        // 校验:节点不能已存在
        if (_nodes.ContainsKey(path))
            throw new InvalidOperationException($"节点已存在: {path}");

        // 顺序节点: 追加单调递增序号
        var actualPath = path;
        if (type is ZNodeType.PersistentSequential or ZNodeType.EphemeralSequential)
        {
            var seq = Interlocked.Increment(ref _sequenceCounter);
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
        _nodes[actualPath] = node;

        // 触发 Watch: 节点创建
        NotifyWatchers(actualPath, WatchEventType.NodeCreated);
        // 触发 Watch: 父节点子节点列表变化
        NotifyWatchers(parentPath, WatchEventType.NodeChildrenChanged);

        return Task.FromResult(actualPath);
    }

    /// <summary>
    /// 读取节点数据
    /// </summary>
    public Task<ZNode?> GetDataAsync(string path, bool watch = false)
    {
        if (!_nodes.TryGetValue(path, out var node))
            return Task.FromResult<ZNode?>(null);

        if (watch)
            RegisterWatcher(path, _ => { });   // 注册空 Watch(真实 ZK 一次性触发)

        return Task.FromResult<ZNode?>(node);
    }

    /// <summary>
    /// 获取子节点列表
    /// </summary>
    public Task<IEnumerable<string>> GetChildrenAsync(string path, bool watch = false)
    {
        if (!_nodes.ContainsKey(path))
            throw new InvalidOperationException($"节点不存在: {path}");

        var children = _nodes.Keys
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

    /// <summary>
    /// 检查节点是否存在
    /// </summary>
    public Task<bool> ExistsAsync(string path, bool watch = false)
    {
        var exists = _nodes.ContainsKey(path);
        if (watch && !exists)
            RegisterWatcher(path, _ => { });
        return Task.FromResult(exists);
    }

    /// <summary>
    /// 修改节点数据(CAS 乐观锁)
    /// </summary>
    public Task SetDataAsync(string path, byte[] data, long expectedVersion = -1)
    {
        if (!_nodes.TryGetValue(path, out var node))
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
        _nodes[path] = updated;

        // 触发 Watch: 数据变化
        NotifyWatchers(path, WatchEventType.NodeDataChanged);

        return Task.CompletedTask;
    }

    /// <summary>
    /// 删除节点(CAS 乐观锁)
    /// </summary>
    public Task DeleteAsync(string path, long expectedVersion = -1)
    {
        if (!_nodes.TryGetValue(path, out var node))
            throw new InvalidOperationException($"节点不存在: {path}");

        if (expectedVersion != -1 && node.Version != expectedVersion)
            throw new InvalidOperationException($"版本不匹配");

        // 校验: 不能删除有子节点的节点
        if (_nodes.Keys.Any(k => k.StartsWith(path + "/")))
            throw new InvalidOperationException($"节点有子节点,不能删除: {path}");

        _nodes.Remove(path);

        // 触发 Watch: 节点删除
        NotifyWatchers(path, WatchEventType.NodeDeleted);
        // 父节点子节点列表变化
        NotifyWatchers(GetParentPath(path), WatchEventType.NodeChildrenChanged);

        return Task.CompletedTask;
    }

    /// <summary>
    /// 注册 Watch 回调
    /// </summary>
    public void RegisterWatcher(string path, Action<WatchEvent> callback)
    {
        if (!_watchers.ContainsKey(path))
            _watchers[path] = new List<Action<WatchEvent>>();
        _watchers[path].Add(callback);
    }

    /// <summary>
    /// 模拟会话失效(临时节点自动删除)
    /// 学习要点: 临时节点的核心特性,客户端断开会话自动删除
    /// </summary>
    public void SimulateSessionExpired()
    {
        IsConnected = false;
        var ephemeralPaths = _nodes
            .Where(kv => kv.Value.EphemeralOwner == SessionId)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var path in ephemeralPaths)
        {
            _nodes.Remove(path);
            NotifyWatchers(path, WatchEventType.NodeDeleted);
            NotifyWatchers(GetParentPath(path), WatchEventType.NodeChildrenChanged);
        }

        Console.WriteLine($"[ZK] 会话 {SessionId} 失效,自动删除 {ephemeralPaths.Count} 个临时节点");
    }

    private void NotifyWatchers(string path, WatchEventType eventType)
    {
        if (_watchers.TryGetValue(path, out var watchers))
        {
            var evt = new WatchEvent(eventType, path);
            // 复制一份避免回调中修改列表
            foreach (var w in watchers.ToList())
            {
                try { w(evt); }
                catch (Exception ex) { Console.WriteLine($"[Watch 错误] {ex.Message}"); }
            }
            // ZK 3.x 的 Watch 是一次性的,触发后自动失效
            // _watchers[path].Clear();  // 学习用,这里不清理以便多次演示
        }
    }

    private static string GetParentPath(string path)
    {
        if (path == "/") return "/";
        var idx = path.LastIndexOf('/');
        return idx <= 0 ? "/" : path.Substring(0, idx);
    }

    public void Dispose() => SimulateSessionExpired();
}
