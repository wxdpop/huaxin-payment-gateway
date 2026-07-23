// ============================================================================
// ZooKeeper C# 客户端抽象
// ============================================================================
// 学习要点:
//   1. 用接口抽象 ZK 客户端,生产可换 ZooKeeperNetEx / Curator 等真实库
//   2. 本 Demo 提供 InMemoryZooKeeper 内存模拟实现,无需启动 ZK 服务即可学习
//   3. 演示核心概念:ZNode 树、四种节点类型、Watch、分布式锁、Leader 选举
//
// 真实环境替换:
//   1. 安装 ZooKeeperNetEx NuGet 包
//   2. Docker 启动 ZK: docker run -d --name zk -p 2181:2181 confluentinc/cp-zookeeper
//   3. 把 InMemoryZooKeeper 换成 ZooKeeperNetEx.ZooKeeper
// ============================================================================

namespace ZooKeeperDemo;

// ============================================================================
// ZNode 节点模型
// ============================================================================

/// <summary>
/// ZNode 节点类型
/// 学习要点: 四种节点类型是 ZK 数据模型的核心
///   - PERSISTENT: 客户端断开后仍存在,需显式删除
///   - PERSISTENT_SEQUENTIAL: 持久 + 自动追加单调递增序号
///   - EPHEMERAL: 客户端会话失效时自动删除(临时节点)
///   - EPHEMERAL_SEQUENTIAL: 临时 + 顺序(分布式锁核心)
/// </summary>
public enum ZNodeType
{
    Persistent,
    PersistentSequential,
    Ephemeral,
    EphemeralSequential
}

/// <summary>
/// ZNode 节点实体
/// </summary>
public record ZNode(
    string Path,                   // 节点完整路径(如 /app1/config)
    byte[] Data,                    // 节点数据(≤ 1MB)
    ZNodeType NodeType,             // 节点类型
    long Version,                   // 数据版本号(CAS 乐观锁)
    long? EphemeralOwner,           // 临时节点的持有会话 ID
    DateTime CreatedAt,
    DateTime ModifiedAt);

// ============================================================================
// Watch 事件
// ============================================================================

public enum WatchEventType
{
    NodeCreated,           // 节点创建
    NodeDeleted,           // 节点删除
    NodeDataChanged,       // 数据变化
    NodeChildrenChanged    // 子节点列表变化
}

public record WatchEvent(
    WatchEventType Type,
    string Path);

// ============================================================================
// ZK 客户端接口
// ============================================================================

/// <summary>
/// ZK 客户端抽象接口
/// 学习要点: 用接口隔离,便于:
///   1. 单元测试 Mock
///   2. 切换不同实现(ZooKeeperNetEx / 自实现)
/// </summary>
public interface IZooKeeperClient : IDisposable
{
    // 节点操作
    Task<string> CreateAsync(string path, byte[] data, ZNodeType type);
    Task<ZNode?> GetDataAsync(string path, bool watch = false);
    Task<IEnumerable<string>> GetChildrenAsync(string path, bool watch = false);
    Task<bool> ExistsAsync(string path, bool watch = false);
    Task SetDataAsync(string path, byte[] data, long expectedVersion = -1);
    Task DeleteAsync(string path, long expectedVersion = -1);

    // Watch 注册
    void RegisterWatcher(string path, Action<WatchEvent> callback);

    // 会话管理
    long SessionId { get; }
    bool IsConnected { get; }
}
