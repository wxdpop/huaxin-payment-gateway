// ============================================================================
// 基于 ZooKeeper 的分布式锁实现
// ============================================================================
// 学习要点:
//   1. 分布式锁是 ZK 最经典的用法
//   2. 核心算法: 所有客户端在 /lock 下创建临时顺序节点
//      - 序号最小者获锁
//      - 其他客户端监听前一个节点(避免惊群)
//   3. 临时节点保证客户端宕机时自动释放锁(防死锁)
//   4. 顺序节点保证公平锁(FIFO)
//
// 对比 Redis 分布式锁:
//   Redis 锁: 性能高,但客户端崩溃需等过期时间,有死锁窗口
//   ZK 锁: 性能略低,但会话失效立即释放,无死锁风险
// ============================================================================

namespace ZooKeeperDemo;

public class ZkDistributedLock : IDisposable
{
    private readonly IZooKeeperClient _zk;
    private readonly string _lockPath;       // 锁根路径(如 /locks/order)
    private string? _myNode;                  // 自己创建的顺序节点路径
    private bool _acquired;

    public ZkDistributedLock(IZooKeeperClient zk, string lockPath)
    {
        _zk = zk;
        _lockPath = lockPath;
    }

    /// <summary>
    /// 尝试获取锁
    /// 算法步骤:
    ///   1. 创建临时顺序节点 /lock/lock_xxxxxxxx
    ///   2. 获取所有子节点,排序
    ///   3. 自己是序号最小 → 获得锁
    ///   4. 不是最小 → 监听前一个节点删除事件
    ///   5. 前一个删除 → 重新检查自己是否最小(可能多个节点同时唤醒)
    /// </summary>
    public async Task<bool> TryAcquireAsync(TimeSpan timeout)
    {
        // 1. 确保锁根路径存在
        await EnsurePathAsync(_lockPath);

        // 2. 创建临时顺序节点(关键字: EPHEMERAL_SEQUENTIAL)
        _myNode = await _zk.CreateAsync(
            $"{_lockPath}/lock_",
            data: Array.Empty<byte>(),
            type: ZNodeType.EphemeralSequential);
        Console.WriteLine($"[Lock] 创建节点: {_myNode}");

        // 3. 循环检查是否获得锁
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await CheckAndAcquireAsync())
                return true;

            // 4. 等待前一个节点删除
            await WaitForPreviousNodeAsync(deadline);
        }

        // 超时,清理自己的节点
        if (_myNode != null)
            await _zk.DeleteAsync(_myNode);
        return false;
    }

    /// <summary>
    /// 检查是否获得锁(序号最小者获锁)
    /// </summary>
    private async Task<bool> CheckAndAcquireAsync()
    {
        // 获取所有子节点,按序号排序
        var children = await _zk.GetChildrenAsync(_lockPath);
        var sorted = children
            .OrderBy(c => c.Substring(c.LastIndexOf('/') + 1))
            .ToList();

        if (sorted.Count == 0)
            return false;

        // 序号最小者获锁
        var minNode = sorted[0];
        if (_myNode == minNode)
        {
            _acquired = true;
            Console.WriteLine($"[Lock] ✓ 获得锁: {_myNode}");
            return true;
        }

        // 找到自己的位置,监听前一个节点
        var myIndex = sorted.IndexOf(_myNode!);
        if (myIndex <= 0) return false;

        var previousNode = sorted[myIndex - 1];
        Console.WriteLine($"[Lock] 等待前一个节点: {previousNode}");
        return false;
    }

    /// <summary>
    /// 等待前一个节点删除
    /// 学习要点: 监听前一个节点(而非所有更小节点),避免惊群
    /// </summary>
    private async Task WaitForPreviousNodeAsync(DateTime deadline)
    {
        var children = await _zk.GetChildrenAsync(_lockPath);
        var sorted = children.OrderBy(c => c.Substring(c.LastIndexOf('/') + 1)).ToList();
        var myIndex = sorted.IndexOf(_myNode!);
        if (myIndex <= 0) return;

        var previousNode = sorted[myIndex - 1];
        var tcs = new TaskCompletionSource<bool>();

        // 注册 Watch: 前一个节点删除时触发
        _zk.RegisterWatcher(previousNode, evt =>
        {
            if (evt.Type == WatchEventType.NodeDeleted)
                tcs.TrySetResult(true);
        });

        // 等待 Watch 触发或超时
        var remaining = deadline - DateTime.UtcNow;
        if (remaining > TimeSpan.Zero)
            await Task.WhenAny(tcs.Task, Task.Delay(remaining));
    }

    /// <summary>
    /// 确保路径存在(递归创建)
    /// </summary>
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

    /// <summary>
    /// 释放锁(删除自己的节点)
    /// 学习要点:
    ///   1. 主动释放 = 删除节点
    ///   2. 客户端宕机 = 会话失效,临时节点自动删除,也释放锁
    /// </summary>
    public void Dispose()
    {
        if (_acquired && _myNode != null)
        {
            try
            {
                _zk.DeleteAsync(_myNode).Wait();
                Console.WriteLine($"[Lock] 释放锁: {_myNode}");
            }
            catch { }
        }
        _acquired = false;
        _myNode = null;
    }
}
