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
// 运行:
//   dotnet run --project examples/ZooKeeperDemo
//
// 学习路径:
//   1. 先读 IZooKeeperClient.cs 理解接口抽象
//   2. 再读 InMemoryZooKeeper.cs 理解 ZK 内部实现
//   3. 然后读 ZkDistributedLock.cs 理解分布式锁算法
//   4. 最后读 ZkLeaderElection.cs 理解选举算法
//   5. 回到本文件看完整 Demo 演示
//
// 真实环境替换:
//   Docker:  docker run -d --name zk -p 2181:2181 confluentinc/cp-zookeeper
//   NuGet:   dotnet add package ZooKeeperNetEx
//   代码:    把 new InMemoryZooKeeper() 替换为 new ZooKeeper("127.0.0.1:2181", ...)
// ============================================================================

using System.Text;
using ZooKeeperDemo;

Console.WriteLine("============================================================");
Console.WriteLine("  ZooKeeper C# Demo");
Console.WriteLine("  (使用 InMemoryZooKeeper 模拟,无需启动真实 ZK 服务)");
Console.WriteLine("============================================================\n");

// ============================================================================
// 场景 1: 基础 CRUD + 四种节点类型
// ============================================================================
Console.WriteLine("【场景 1】基础 CRUD + 四种节点类型");
Console.WriteLine("------------------------------------------------------------");

using var zk = new InMemoryZooKeeper();
Console.WriteLine($"已连接,SessionId: {zk.SessionId}\n");

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
Console.WriteLine();

// ============================================================================
// 场景 3: 分布式锁(三个客户端竞争)
// ============================================================================
Console.WriteLine("【场景 3】分布式锁 - 三个客户端竞争");
Console.WriteLine("------------------------------------------------------------");

// 三个客户端模拟(实际环境是三个进程或三台机器)
var clients = new[] { new InMemoryZooKeeper(), new InMemoryZooKeeper(), new InMemoryZooKeeper() };
var locks = clients.Select(c => new ZkDistributedLock(c, "/locks/order")).ToArray();

// 模拟客户端 1 先获得锁
Console.WriteLine("客户端 1 尝试获取锁...");
var acquired1 = await locks[0].TryAcquireAsync(TimeSpan.FromSeconds(5));
Console.WriteLine($"客户端 1 获锁: {acquired1}\n");

// 客户端 2 尝试获锁(会等待)
Console.WriteLine("客户端 2 尝试获取锁...");
var task2 = locks[1].TryAcquireAsync(TimeSpan.FromSeconds(5));
Console.WriteLine("客户端 3 尝试获取锁...");
var task3 = locks[2].TryAcquireAsync(TimeSpan.FromSeconds(5));

// 客户端 1 释放锁
await Task.Delay(500);
Console.WriteLine("\n客户端 1 释放锁...");
locks[0].Dispose();

// 等待客户端 2 或 3 获得锁
await Task.WhenAll(task2, task3);
Console.WriteLine($"\n客户端 2 获锁: {task2.Result}");
Console.WriteLine($"客户端 3 获锁: {task3.Result}");

// 清理
locks[1].Dispose();
locks[2].Dispose();
foreach (var c in clients) c.Dispose();
Console.WriteLine();

// ============================================================================
// 场景 4: Leader 选举
// ============================================================================
Console.WriteLine("【场景 4】Leader 选举");
Console.WriteLine("------------------------------------------------------------");

using var zk2 = new InMemoryZooKeeper();
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

// 等待新 Leader 选举完成
await Task.Delay(500);
var newLeader = electors.FirstOrDefault(e => e.IsLeader);
if (newLeader != null)
    Console.WriteLine($"新 Leader: {newLeader.NodeId}");

foreach (var e in electors) e.Dispose();
Console.WriteLine();

// ============================================================================
// 场景 5: 临时节点自动清理
// ============================================================================
Console.WriteLine("【场景 5】临时节点自动清理(会话失效)");
Console.WriteLine("------------------------------------------------------------");

using var zk3 = new InMemoryZooKeeper();
await zk3.CreateAsync("/services", Array.Empty<byte>(), ZNodeType.Persistent);
await zk3.CreateAsync("/services/payment-service", Array.Empty<byte>(), ZNodeType.Persistent);
await zk3.CreateAsync("/services/payment-service/instance_001", Encoding.UTF8.GetBytes("192.168.1.10:5001"), ZNodeType.Ephemeral);
await zk3.CreateAsync("/services/payment-service/instance_002", Encoding.UTF8.GetBytes("192.168.1.11:5001"), ZNodeType.Ephemeral);

var instances = await zk3.GetChildrenAsync("/services/payment-service");
Console.WriteLine($"注册实例数: {instances.Count()}");
foreach (var inst in instances)
    Console.WriteLine($"  - {inst}");

Console.WriteLine("\n模拟服务实例 1 宕机(会话失效)...");
zk3.SimulateSessionExpired();

// 检查临时节点是否被自动清理
var remaining = await zk3.GetChildrenAsync("/services/payment-service");
Console.WriteLine($"会话失效后剩余实例数: {remaining.Count()} (临时节点应被自动清理)");
Console.WriteLine($"持久节点 /services/payment-service 仍存在: {await zk3.ExistsAsync("/services/payment-service")}");

Console.WriteLine("\n============================================================");
Console.WriteLine("  Demo 完成,学习要点回顾:");
Console.WriteLine("  1. 四种节点类型(持久/顺序/临时/临时顺序)");
Console.WriteLine("  2. Watch 机制(数据变化通知)");
Console.WriteLine("  3. 分布式锁(临时顺序节点 + 监听前一个节点)");
Console.WriteLine("  4. Leader 选举(序号最小者当选,链式监听避免惊群)");
Console.WriteLine("  5. 临时节点会话失效自动删除(防死锁/服务下线)");
Console.WriteLine("============================================================");
