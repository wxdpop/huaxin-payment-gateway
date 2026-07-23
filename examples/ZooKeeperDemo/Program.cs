// ============================================================================
// ZooKeeper Demo 主入口
// ============================================================================
// 演示场景:
//   1. 基础 CRUD(创建/读取/修改/删除四种节点类型)
//   2. Watch 监听机制(数据变化通知)
//   3. 分布式锁(多个客户端竞争锁)
//   4. Leader 选举(多节点选主)
//   5. 临时节点自动清理(会话失效)
//
// 运行模式:
//   1. InMemory 模式(默认):  无需 ZK 服务,内存模拟
//      dotnet run --project examples/ZooKeeperDemo
//   2. Real 模式:             连接真实 ZK 服务(需 Docker 启动)
//      dotnet run --project examples/ZooKeeperDemo -- real
//      或显式指定地址: dotnet run --project examples/ZooKeeperDemo -- real 127.0.0.1:2181
//
// Docker 启动 ZK:
//   docker run -d --name zk -p 2181:2181 confluentinc/cp-zookeeper
//
// 学习路径:
//   1. 先读 IZooKeeperClient.cs 理解接口抽象
//   2. 再读 InMemoryZooKeeper.cs 理解 ZK 内部实现(内存模拟版)
//   3. 然后读 ZkDistributedLock.cs 理解分布式锁算法
//   4. 接着读 ZkLeaderElection.cs 理解选举算法
//   5. 读 RealZooKeeperClient.cs 理解 ZooKeeperNetEx 真实客户端适配
//   6. 回到本文件看完整 Demo 演示
// ============================================================================

using System.Text;
using ZooKeeperDemo;

// ============================================================================
// 解析命令行参数,选择运行模式
// ============================================================================
// 学习要点: dotnet run -- 后的参数会传递给 Main
//   -- 后第一个参数是 mode(real/memory),第二个是 ZK 地址(可选)
var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "memory";
var connectString = args.Length > 1 ? args[1] : "127.0.0.1:2181";

bool useReal = mode is "real" or "r";
Console.WriteLine("============================================================");
Console.WriteLine($"  ZooKeeper C# Demo  ({(useReal ? $"真实模式 -> {connectString}" : "InMemory 内存模拟模式")})");
Console.WriteLine("============================================================\n");

// 工厂方法:统一构造客户端
// 学习要点: Real 模式用 5 秒 sessionTimeout 加速临时节点清理演示
//   - 真实生产环境通常用 30s,本 Demo 用 5s 加速场景 5 的"会话失效"演示
//   - ZK 服务端在 sessionTimeout 后才会清理临时节点
IZooKeeperClient CreateClient() =>
    useReal ? new RealZooKeeperClient(connectString, 5) : new InMemoryZooKeeper();

// 学习要点: InMemory 模式下静态共享存储(模拟 ZK 服务器)
//   重置后所有 InMemoryZooKeeper 客户端从空 ZNode 树开始
//   避免上一次运行残留数据干扰本次演示
if (!useReal)
    InMemoryZooKeeper.ResetSharedStore();

// ============================================================================
// 场景 1: 基础 CRUD + 四种节点类型
// ============================================================================
Console.WriteLine("【场景 1】基础 CRUD + 四种节点类型");
Console.WriteLine("------------------------------------------------------------");

using var zk = CreateClient();
Console.WriteLine($"已连接,SessionId: {zk.SessionId}\n");

// 先清理历史数据(real 模式下可能残留前次运行的节点)
await CleanupAsync(zk, "/app1");

// 持久节点
await zk.CreateAsync("/app1", Array.Empty<byte>(), ZNodeType.Persistent);
var persistentPath = await zk.CreateAsync("/app1/config", Encoding.UTF8.GetBytes("jdbc:mysql://..."), ZNodeType.Persistent);
var data = await zk.GetDataAsync("/app1/config");
Console.WriteLine($"读取持久节点: {Encoding.UTF8.GetString(data!.Data)}");

// 持久顺序节点(自动追加序号)
await zk.CreateAsync("/app1/queue", Array.Empty<byte>(), ZNodeType.Persistent);
var seq1 = await zk.CreateAsync("/app1/queue/item-", Encoding.UTF8.GetBytes("msg1"), ZNodeType.PersistentSequential);
var seq2 = await zk.CreateAsync("/app1/queue/item-", Encoding.UTF8.GetBytes("msg2"), ZNodeType.PersistentSequential);
Console.WriteLine($"顺序节点 1: {seq1}");
Console.WriteLine($"顺序节点 2: {seq2}");

// 临时节点(会话失效自动删)
var ephemeralPath = await zk.CreateAsync("/app1/lock_temp", Encoding.UTF8.GetBytes("locked"), ZNodeType.Ephemeral);
Console.WriteLine($"临时节点: {ephemeralPath}");

// 修改数据(CAS 版本)
await zk.SetDataAsync("/app1/config", Encoding.UTF8.GetBytes("jdbc:postgresql://..."));
var updated = await zk.GetDataAsync("/app1/config");
Console.WriteLine($"修改后: {Encoding.UTF8.GetString(updated!.Data)}, Version={updated.Version}");

// 子节点列表
var children = await zk.GetChildrenAsync("/app1");
Console.WriteLine($"/app1 子节点: {string.Join(", ", children)}\n");

// ============================================================================
// 场景 2: Watch 监听机制
// ============================================================================
Console.WriteLine("【场景 2】Watch 监听机制");
Console.WriteLine("------------------------------------------------------------");

// 注册 Watch
zk.RegisterWatcher("/app1/config", evt =>
{
    Console.WriteLine($"  [Watch 触发] Type={evt.Type}, Path={evt.Path}");
});

Console.WriteLine("修改 /app1/config 触发 Watch...");
await zk.SetDataAsync("/app1/config", Encoding.UTF8.GetBytes("new value"));
// 等待 Watch 异步触发(真实 ZK 是网络往返,需要等待)
await Task.Delay(useReal ? 500 : 100);
Console.WriteLine();

// ============================================================================
// 场景 3: 分布式锁(三个客户端竞争,FIFO 公平锁)
// ============================================================================
Console.WriteLine("【场景 3】分布式锁 - 三个客户端竞争(FIFO 公平锁)");
Console.WriteLine("------------------------------------------------------------");

// 清理历史数据
await CleanupAsync(zk, "/locks/order");

// 三个客户端模拟(实际环境是三个进程或三台机器)
var clients = new[] { CreateClient(), CreateClient(), CreateClient() };
var locks = clients.Select(c => new ZkDistributedLock(c, "/locks/order")).ToArray();

// 学习要点: 分布式锁的 FIFO 公平性
//   - 三个客户端依次请求锁,顺序为 1 -> 2 -> 3
//   - 客户端 1 立即获得锁(序号最小)
//   - 客户端 2/3 阻塞等待,按请求顺序依次获得
//   - 任意时刻只有一个客户端持有锁(互斥性)

// 客户端 1 先获得锁
Console.WriteLine("客户端 1 尝试获取锁...");
var acquired1 = await locks[0].TryAcquireAsync(TimeSpan.FromSeconds(10));
Console.WriteLine($"客户端 1 获锁: {acquired1}");

// 客户端 2/3 后续请求(会阻塞等待,Watch 监听前一个节点)
Console.WriteLine("\n客户端 2 尝试获取锁(将等待)...");
var task2 = locks[1].TryAcquireAsync(TimeSpan.FromSeconds(15));
await Task.Delay(useReal ? 300 : 100);  // 确保 2 先注册 Watch
Console.WriteLine("客户端 3 尝试获取锁(将等待)...");
var task3 = locks[2].TryAcquireAsync(TimeSpan.FromSeconds(15));

// 等待一小段时间,确认 2/3 都在阻塞
await Task.Delay(useReal ? 500 : 200);
Console.WriteLine("\n(此时客户端 2/3 都在等待,客户端 1 持有锁)");

// 客户端 1 释放锁 → 客户端 2 被唤醒获得锁
Console.WriteLine("\n客户端 1 释放锁...");
locks[0].Dispose();

// 等待客户端 2 获得锁(Watch 触发有网络往返,Real 模式需更久)
await Task.Delay(useReal ? 1000 : 300);
var client2Acquired = task2.IsCompleted ? task2.Result : false;
Console.WriteLine($"客户端 2 获锁: {client2Acquired}");

// 客户端 2 释放锁 → 客户端 3 被唤醒获得锁
Console.WriteLine("\n客户端 2 释放锁...");
locks[1].Dispose();

await Task.Delay(useReal ? 1000 : 300);
var client3Acquired = task3.IsCompleted ? task3.Result : false;
Console.WriteLine($"客户端 3 获锁: {client3Acquired}");

// 清理
locks[2].Dispose();
foreach (var c in clients) c.Dispose();
Console.WriteLine();

// ============================================================================
// 场景 4: Leader 选举
// ============================================================================
Console.WriteLine("【场景 4】Leader 选举");
Console.WriteLine("------------------------------------------------------------");

// 清理历史数据
await CleanupAsync(zk, "/election");

using var zk2 = CreateClient();
var electors = new[]
{
    new ZkLeaderElection(zk2, "/election", "Node-A"),
    new ZkLeaderElection(zk2, "/election", "Node-B"),
    new ZkLeaderElection(zk2, "/election", "Node-C")
};

// 启动三个节点参与选举
foreach (var e in electors)
    await e.StartAsync();

Console.WriteLine($"当前 Leader: {electors.First(e => e.IsLeader).NodeId}");

// 模拟 Leader 宕机
Console.WriteLine("\n模拟 Leader 宕机...");
var currentLeader = electors.First(e => e.IsLeader);
currentLeader.Dispose();

// 等待新 Leader 选举完成(真实 ZK 需要 Watch 网络往返)
await Task.Delay(useReal ? 1000 : 500);
var newLeader = electors.FirstOrDefault(e => e.IsLeader);
if (newLeader != null)
    Console.WriteLine($"新 Leader: {newLeader.NodeId}");

foreach (var e in electors) e.Dispose();
Console.WriteLine();

// ============================================================================
// 场景 5: 临时节点自动清理(会话失效)
// ============================================================================
Console.WriteLine("【场景 5】临时节点自动清理(会话失效)");
Console.WriteLine("------------------------------------------------------------");

// 清理历史数据
await CleanupAsync(zk, "/services");

using var zk3 = CreateClient();
await zk3.CreateAsync("/services", Array.Empty<byte>(), ZNodeType.Persistent);
await zk3.CreateAsync("/services/payment-service", Array.Empty<byte>(), ZNodeType.Persistent);

// 用一个临时客户端注册实例(模拟服务实例上线)
var instanceClient = CreateClient();
await instanceClient.CreateAsync("/services/payment-service/instance_001", Encoding.UTF8.GetBytes("192.168.1.10:5001"), ZNodeType.Ephemeral);
await instanceClient.CreateAsync("/services/payment-service/instance_002", Encoding.UTF8.GetBytes("192.168.1.11:5001"), ZNodeType.Ephemeral);

var instances = await zk3.GetChildrenAsync("/services/payment-service");
Console.WriteLine($"注册实例数: {instances.Count()}");
foreach (var inst in instances)
    Console.WriteLine($"  - {inst}");

Console.WriteLine("\n模拟服务实例 1 宕机(关闭会话)...");
// 学习要点: 两种"会话失效"方式:
//   InMemory 模式: SimulateSessionExpired() 立即清理
//   Real 模式: Dispose 客户端触发 closeAsync,ZK 服务端在 sessionTimeout 后清理临时节点
if (instanceClient is InMemoryZooKeeper inMem)
    inMem.SimulateSessionExpired();
else
    instanceClient.Dispose();

// 等待临时节点被清理(Real 模式需等待 ZK 服务端检测到会话失效)
// ★ 学习要点: Real 模式下临时节点清理有延迟
//   - 客户端关闭会话后,ZK 服务端在 sessionTimeout(本 Demo 设为 5s)后清理
//   - 真实生产环境通常 sessionTimeout=30s,清理延迟更长
if (useReal)
{
    Console.WriteLine("等待 ZK 服务端清理临时节点(最长 15s)...");
    var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
    while (DateTime.UtcNow < deadline)
    {
        var remaining = await zk3.GetChildrenAsync("/services/payment-service");
        if (!remaining.Any()) break;
        await Task.Delay(1000);
        Console.Write(".");
    }
    Console.WriteLine();
}

// 检查临时节点是否被自动清理
var remainingInstances = await zk3.GetChildrenAsync("/services/payment-service");
Console.WriteLine($"会话失效后剩余实例数: {remainingInstances.Count()} (临时节点应被自动清理)");
Console.WriteLine($"持久节点 /services/payment-service 仍存在: {await zk3.ExistsAsync("/services/payment-service")}");

if (instanceClient is not InMemoryZooKeeper)
    instanceClient.Dispose();

Console.WriteLine("\n============================================================");
Console.WriteLine("  Demo 完成,学习要点回顾:");
Console.WriteLine("  1. 四种节点类型(持久/顺序/临时/临时顺序)");
Console.WriteLine("  2. Watch 机制(数据变化通知,一次性触发)");
Console.WriteLine("  3. 分布式锁(临时顺序节点 + 监听前一个节点)");
Console.WriteLine("  4. Leader 选举(序号最小者当选,链式监听避免惊群)");
Console.WriteLine("  5. 临时节点会话失效自动删除(防死锁/服务下线)");
if (useReal)
{
    Console.WriteLine("\n  真实模式注意点:");
    Console.WriteLine("    - Watch 是一次性的,触发后需重新注册(本 Demo 已自动处理)");
    Console.WriteLine("    - 临时节点清理有延迟(等 sessionTimeout)");
    Console.WriteLine("    - 多个 RealZooKeeperClient 模拟多机部署");
}
Console.WriteLine("============================================================");

// ============================================================================
// 辅助函数:递归清理节点(real 模式下避免历史数据干扰)
// ============================================================================
async Task CleanupAsync(IZooKeeperClient client, string rootPath)
{
    // 学习要点: 两种模式下都需清理,避免历史数据干扰
    //   InMemory: 静态共享存储,需要清理本次运行残留
    //   Real: ZK 服务持久化,前次运行的节点会残留
    if (!await client.ExistsAsync(rootPath)) return;
    await DeleteRecursiveAsync(client, rootPath);
}

async Task DeleteRecursiveAsync(IZooKeeperClient client, string path)
{
    var children = await client.GetChildrenAsync(path);
    foreach (var child in children)
    {
        // 学习要点: GetChildrenAsync 返回的是完整路径(如 /locks/order/lock_001)
        //   不是相对名称,所以直接递归
        await DeleteRecursiveAsync(client, child);
    }
    try { await client.DeleteAsync(path); } catch { /* 忽略 */ }
}
