// ============================================================================
// 基于 ZooKeeper 的 Leader 选举实现
// ============================================================================
// 学习要点:
//   1. Leader 选举是 ZK 经典用法(Kafka Controller / Hadoop NameNode 都用)
//   2. 算法与分布式锁类似:
//      - 所有节点创建临时顺序节点
//      - 序号最小者当选 Leader
//      - 其他节点监听前一个节点
//   3. Leader 宕机 → 节点删除 → 下一个最小者被唤醒 → 新 Leader
//
// 对比分布式锁:
//   锁: 短期持有,用完释放
//   选举: 长期持有,直到宕机
// ============================================================================

namespace ZooKeeperDemo;

public class ZkLeaderElection : IDisposable
{
    private readonly IZooKeeperClient _zk;
    private readonly string _electionPath;
    private readonly string _nodeId;
    private string? _myNode;
    private bool _isLeader;

    public bool IsLeader => _isLeader;
    public string NodeId => _nodeId;

    /// <summary>
    /// Leader 变更事件
    /// </summary>
    public event Action<bool>? LeaderChanged;

    public ZkLeaderElection(IZooKeeperClient zk, string electionPath, string nodeId)
    {
        _zk = zk;
        _electionPath = electionPath;
        _nodeId = nodeId;
    }

    /// <summary>
    /// 参与 Leader 选举
    /// </summary>
    public async Task StartAsync()
    {
        // 确保选举路径存在
        await EnsurePathAsync(_electionPath);

        // 创建临时顺序节点(包含节点 ID 便于识别)
        var data = System.Text.Encoding.UTF8.GetBytes(_nodeId);
        _myNode = await _zk.CreateAsync(
            $"{_electionPath}/node_",
            data,
            type: ZNodeType.EphemeralSequential);
        Console.WriteLine($"[{_nodeId}] 创建选举节点: {_myNode}");

        // 检查是否当选
        await CheckLeadershipAsync();
    }

    /// <summary>
    /// 检查是否是 Leader
    /// </summary>
    private async Task CheckLeadershipAsync()
    {
        var children = await _zk.GetChildrenAsync(_electionPath);
        var sorted = children.OrderBy(c => c.Substring(c.LastIndexOf('/') + 1)).ToList();

        if (sorted.Count == 0) return;

        // 序号最小者当选
        var leaderNode = sorted[0];
        if (_myNode == leaderNode)
        {
            _isLeader = true;
            Console.WriteLine($"[{_nodeId}] ★ 当选 Leader: {_myNode}");
            LeaderChanged?.Invoke(true);
            return;
        }

        // 不是最小 → 监听前一个节点
        var myIndex = sorted.IndexOf(_myNode!);
        if (myIndex > 0)
        {
            var previousNode = sorted[myIndex - 1];
            Console.WriteLine($"[{_nodeId}] 当前为 Follower,监听前一个节点: {previousNode}");

            _zk.RegisterWatcher(previousNode, async evt =>
            {
                if (evt.Type == WatchEventType.NodeDeleted)
                {
                    Console.WriteLine($"[{_nodeId}] 前一个节点 {previousNode} 已删除,重新选举");
                    await CheckLeadershipAsync();
                }
            });
        }
    }

    private async Task EnsurePathAsync(string path)
    {
        var parts = path.Trim('/').Split('/');
        var current = "";
        foreach (var part in parts)
        {
            current = current + "/" + part;
            if (!await _zk.ExistsAsync(current))
                await _zk.CreateAsync(current, Array.Empty<byte>(), ZNodeType.Persistent);
        }
    }

    public void Dispose()
    {
        if (_myNode != null)
        {
            try { _zk.DeleteAsync(_myNode).Wait(); }
            catch { }
        }
        _isLeader = false;
    }
}
