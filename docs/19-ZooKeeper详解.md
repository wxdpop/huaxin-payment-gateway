# ZooKeeper 专项学习文档

> 面向 .NET 工程师的 ZooKeeper 深度学习，含 C# 实战 Demo（InMemory 模拟 + Real 真实连接）。
>
> **本文档重点**：第 8 章「分布式锁深度剖析」是核心，配合 `examples/ZooKeeperDemo` 代码学习。

---

## 目录

1. [ZooKeeper 是什么](#1-zookeeper-是什么)
2. [数据模型](#2-数据模型)
3. [集群架构](#3-集群架构)
4. [Watcher 监听机制](#4-watcher-监听机制)
5. [ACL 权限控制](#5-acl-权限控制)
6. [典型应用模式](#6-典型应用模式)
7. [C# 客户端使用（ZooKeeperNetEx）](#7-c-客户端使用zookeepernetex)
8. [**分布式锁深度剖析（重点）**](#8-分布式锁深度剖析重点)
9. [运维要点](#9-运维要点)
10. [Real 模式连接与踩坑记录](#10-real-模式连接与踩坑记录)
11. [常见问题](#11-常见问题)
12. [面试话术（12 道 Q&A）](#12-面试话术12-道-qa)
13. [学习路径](#13-学习路径)
14. [参考资源](#14-参考资源)

---

## 1. ZooKeeper 是什么

**ZooKeeper（简称 ZK）** 是 Apache 基金会的**分布式协调服务**，为分布式应用提供统一的数据同步、配置管理、命名服务、集群管理能力。

### 1.1 定位

| 维度 | 说明 |
|------|------|
| **作者** | Yahoo! 开发，后捐赠 Apache |
| **起源** | Google Chubby（论文级）的开源实现 |
| **语言** | Java 编写，跨语言客户端 |
| **协议** | 私有 TCP 协议（非 HTTP） |
| **典型用途** | 配置中心、注册中心、分布式锁、Leader 选举、队列 |
| **类似产品** | etcd（Go）、Consul（Go）、Nacos（阿里） |

### 1.2 与 etcd / Consul / Nacos 对比

| 维度 | ZooKeeper | etcd | Consul | Nacos |
|------|-----------|------|--------|-------|
| **语言** | Java | Go | Go | Java |
| **协议** | ZAB（Paxos 变种） | Raft | Raft | Raft + Distro |
| **数据模型** | 树形 ZNode | KV + 前缀 | KV | KV + 服务 |
| **强一致性** | CP | CP | CP | AP/CP 可切换 |
| **Watch 机制** | 一次性 Watch | 持久 Watch | 长轮询 | 长轮询 |
| **典型生态** | Kafka / Hadoop | K8s | 微服务注册 | Spring Cloud 阿里系 |

### 1.3 应用场景

```
┌────────────────────────────────────────────┐
│           ZooKeeper 应用场景                │
├────────────────────────────────────────────┤
│  1. 配置中心   - 多服务共享配置             │
│  2. 注册中心   - 服务发现                   │
│  3. 分布式锁   - 跨进程互斥（★本文重点）    │
│  4. Leader 选举 - 集群主从切换              │
│  5. 命名服务   - 全局唯一 ID 生成           │
│  6. 队列       - 顺序节点实现 FIFO 队列     │
│  7. 集群管理   - 节点存活检测               │
└────────────────────────────────────────────┘
```

---

## 2. 数据模型

### 2.1 ZNode 树形结构

ZK 数据存储为**树形结构**，每个节点叫 **ZNode**：

```
/                              ← 根节点
├── /app1                      ← 持久节点
│   ├── /app1/config           ← 持久节点
│   │   ├── /app1/config/db_url    = "jdbc:..."
│   │   └── /app1/config/timeout   = "30"
│   └── /app1/members           ← 持久节点
│       ├── /app1/members/node_001  ← 临时节点(会话失效自动删)
│       ├── /app1/members/node_002
│       └── /app1/members/node_003
├── /app2
│   └── /app2/locks
│       └── /app2/locks/lock_001   ← 临时顺序节点(分布式锁)
└── /zookeeper                  ← ZK 内置节点(不可删)
    └── /zookeeper/quota
```

### 2.2 ZNode 四种类型

| 类型 | 说明 | 创建方式 | 用途 |
|------|------|---------|------|
| **持久节点（PERSISTENT）** | 客户端断开后仍存在 | `create /path data` | 配置、目录 |
| **持久顺序节点（PERSISTENT_SEQUENTIAL）** | 持久 + 自动追加单调递增序号 | `create -s /path data` | FIFO 队列 |
| **临时节点（EPHEMERAL）** | 客户端会话失效自动删除 | `create -e /path data` | 服务注册、锁 |
| **临时顺序节点（EPHEMERAL_SEQUENTIAL）** | 临时 + 顺序 | `create -e -s /path data` | ★分布式锁核心 |

> **★ 关键理解**：分布式锁的"防死锁"完全依赖临时节点特性——客户端宕机 → 会话失效 → 临时节点自动删除 → 锁自动释放。这是 ZK 锁优于 Redis 锁的核心点。

### 2.3 ZNode 数据结构（Stat）

每个 ZNode 包含一个 `Stat` 结构记录元数据：

| 字段 | 说明 | C# API（ZooKeeperNetEx） |
|------|------|--------------------------|
| `czxid` | 创建事务 ID | `stat.getCzxid()` |
| `mzxid` | 最后修改事务 ID | `stat.getMzxid()` |
| `version` | 数据版本（CAS 乐观锁） | `stat.getVersion()` |
| `cversion` | 子节点版本 | `stat.getCversion()` |
| `ephemeralOwner` | 临时节点持有者会话 ID | `stat.getEphemeralOwner()` |
| `dataLength` | 数据长度 | `stat.getDataLength()` |
| `numChildren` | 子节点数量 | `stat.getNumChildren()` |

> **★ 踩坑提醒**：ZooKeeperNetEx 的 `Stat` 字段是非 public 的，必须用 Java 风格的 getter 方法访问（如 `getVersion()` 而非 `stat.Version`）。

---

## 3. 集群架构

### 3.1 角色

```
┌──────────────────────────────────────────────────┐
│              ZooKeeper 集群架构                   │
├──────────────────────────────────────────────────┤
│                                                   │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│   │ Leader   │  │ Follower │  │ Follower │      │
│   │ (写唯一) │←─│ (读+投票)│←─│ (读+投票)│      │
│   └────┬─────┘  └────┬─────┘  └────┬─────┘      │
│        │              │             │             │
│        └──────────────┴─────────────┘             │
│              ZAB 协议同步数据                      │
│                                                   │
└──────────────────────────────────────────────────┘

  Leader:   处理所有写请求,提案给 Follower
  Follower: 处理读请求,参与 Leader 选举和数据同步
  Observer: 不参与选举,仅扩展读性能(可选)
```

### 3.2 ZAB 协议

**ZooKeeper Atomic Broadcast** 是 ZK 的核心一致性协议：

**两个阶段**：
1. **Leader 选举**：启动或 Leader 宕机时，Follower 投票选新 Leader
2. **原子广播**：所有写请求经 Leader，通过两阶段提交到 Follower

**写流程**：
```
Client → Leader → 生成 ZXID → 发 Proposal 给所有 Follower
                              ← Follower 返回 ACK
Leader 收到过半 ACK → 发 Commit 给所有 Follower
                    → 返回成功给 Client
```

### 3.3 N+1 容错

| 集群规模 | 容忍故障数 | 备注 |
|---------|----------|------|
| 3 节点 | 1 | 推荐（最低可用） |
| 5 节点 | 2 | 生产推荐 |
| 7 节点 | 3 | 大规模 |

> **必须是奇数节点**：因为过半同意才生效，偶数节点没有容错优势反而浪费资源。
> 例如 4 节点仍只能容忍 1 故障（需 3 票过半），不如 3 节点。

---

## 4. Watcher 监听机制

### 4.1 一次性 Watch（ZK 3.x）

```csharp
// 注册 Watch（ZooKeeperNetEx 真实 API）
await _zk.existsAsync("/config", new MyWatcher());

// Watch 触发后失效,需要重新注册
public class MyWatcher : Watcher
{
    public override Task process(WatchedEvent @event)
    {
        if (@event.getType() == Watcher.Event.EventType.NodeDataChanged)
        {
            Console.WriteLine($"数据变化: {@event.getPath()}");
            // ⚠ 重新注册(并发可能漏通知)
            await _zk.getDataAsync("/config", true);
        }
        return Task.CompletedTask;
    }
}
```

**问题**：重新注册期间的数据变化可能丢失。

### 4.2 持久 Watch（ZK 3.6+）

```csharp
// 注册一次,持续监听
zk.addWatch("/config", watcher, AddWatchMode.PERSISTENT);
```

### 4.3 三种 Watch 类型

| 类型 | 触发条件 |
|------|---------|
| **GetData Watch** | 节点数据变化或被删除 |
| **GetChildren Watch** | 子节点列表变化（新增/删除） |
| **Exists Watch** | 节点被创建/删除/数据变化 |

### 4.4 Watch 事件类型枚举

```
Watcher.Event.EventType:
  None                ← 连接状态变化(如 SyncConnected / Disconnected)
  NodeCreated         ← 节点被创建
  NodeDeleted         ← 节点被删除（★分布式锁核心事件）
  NodeDataChanged     ← 节点数据变化
  NodeChildrenChanged ← 子节点列表变化

Watcher.Event.KeeperState:
  SyncConnected      ← 已连接
  Disconnected       ← 断开
  Expired            ← 会话过期（★必须重建 ZooKeeper 实例）
```

---

## 5. ACL 权限控制

ZK 支持权限控制（类似 UNIX 文件权限）：

| 权限 | 缩写 | 说明 |
|------|------|------|
| CREATE | c | 创建子节点 |
| READ | r | 读取节点数据/子节点列表 |
| WRITE | w | 修改节点数据 |
| DELETE | d | 删除子节点 |
| ADMIN | a | 设置权限 |

**认证方案**：
- `world:anyone` - 任何人（默认，即 `ZooDefs.Ids.OPEN_ACL_UNSAFE`）
- `auth:user:pwd` - 用户密码认证
- `digest` - SHA1 摘要认证

---

## 6. 典型应用模式

### 6.1 分布式锁（详见第 8 章）

**核心思想**：所有客户端在 `/lock` 下创建**临时顺序节点**，序号最小者获得锁。

```
1. 创建临时顺序节点 /lock/lock_xxxxxxxx
2. 获取所有子节点,排序
3. 序号最小 → 获得锁
4. 不是最小 → 监听前一个节点(避免惊群)
5. 前一个删除 → 被唤醒 → 重新检查
```

> **本节仅给出概念，第 8 章会做深度剖析**。

### 6.2 Leader 选举

```
步骤:
1. 所有节点创建临时顺序节点 /election/node_xxx
2. 检查自己是否最小节点
3. 最小者成为 Leader,其他监听前一个节点
4. Leader 宕机 → 节点删除 → 下一个被唤醒 → 成为新 Leader
```

**项目代码**：[ZkLeaderElection.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/ZkLeaderElection.cs)

### 6.3 配置中心

```
/config
├── /config/db_url        = "jdbc:mysql://..."
├── /config/db_password   = "encoded..."
└── /config/timeout       = "30"

所有服务在启动时读取配置,并注册 Watch
配置变更 → 触发 Watch → 服务重新读取 → 应用新配置
```

### 6.4 服务注册与发现

```
/services
├── /services/payment-service
│   ├── /services/payment-service/instance_001  = "192.168.1.10:5001"
│   ├── /services/payment-service/instance_002  = "192.168.1.11:5001"
│   └── /services/payment-service/instance_003  = "192.168.1.12:5001"
└── /services/order-service
    └── /services/order-service/instance_001    = "192.168.1.20:5002"

实例启动 → 创建临时节点(会话失效自动删)
消费方:监听 /services/payment-service 子节点变化
```

> **★ 本项目场景 5 即演示此模式**：服务实例下线 → 临时节点自动清理 → 调用方感知。

---

## 7. C# 客户端使用（ZooKeeperNetEx）

### 7.1 主流库对比

| 库 | 命名空间 | 维护状态 | 备注 |
|----|----------|---------|------|
| **ZooKeeperNet** | `ZooKeeperNet` | 停滞 | 老牌，同步 API |
| **ZooKeeperNetEx** ★ | `org.apache.zookeeper` | 活跃 | 本项目使用，API 与 Java 客户端一致 |
| **curator-zookeeper** | - | - | Netflix Curator 风格封装 |
| **DotNetty + 自实现** | - | - | 性能好但开发量大 |

**本项目选择 ZooKeeperNetEx 3.4.12.4**：
```xml
<PackageReference Include="ZooKeeperNetEx" Version="3.4.12.4" />
```

### 7.2 ZooKeeperNetEx API 关键特性

> **★ 重要**：与 ZooKeeperNet 不同，ZooKeeperNetEx 的 API 风格与 Java 客户端几乎一致：

1. **命名空间是 `org.apache.zookeeper`**（不是 `ZooKeeperNet`）
2. **`Watcher` 是抽象类**（不是接口），方法名小写 `process(WatchedEvent)`
3. **所有操作都是 async 版本**（`createAsync`/`getDataAsync`/`existsAsync`），无同步版本
4. **`getDataAsync` 返回 `DataResult`**（含 `.Data` + `.Stat`）
5. **`getChildrenAsync` 返回 `ChildrenResult`**（含 `.Children` + `.Stat`）
6. **`Stat` 字段非 public**，需用 Java 风格 getter：`getVersion()`/`getEphemeralOwner()`/`getCtime()`/`getMtime()`
7. **`getSessionId()`/`getState()` 是方法**（不是属性）
8. **`ZooDefs.Ids.OPEN_ACL_UNSAFE` 是静态字段**（`List<ACL>` 类型）

### 7.3 连接 ZK

```csharp
using org.apache.zookeeper;
using org.apache.zookeeper.data;
using ZooKeeperNetExZooKeeper = org.apache.zookeeper.ZooKeeper;

// 构造函数 4 个参数:
//   connectString        - ZK 地址(集群用逗号分隔)
//   sessionTimeoutMs     - 会话超时(超时后清理临时节点)
//   defaultWatcher       - 默认 Watcher(连接事件)
//   canBeReadOnly        - 集群分区时是否允许只读
var zk = new ZooKeeperNetExZooKeeper(
    "127.0.0.1:2181",
    (int)TimeSpan.FromSeconds(30).TotalMilliseconds,
    new ConnectionWatcher(),
    canBeReadOnly: false);

// 等待连接建立(ZK 构造函数立即返回,连接是异步的)
public class ConnectionWatcher : Watcher
{
    public override Task process(WatchedEvent @event)
    {
        if (@event.getState() == Watcher.Event.KeeperState.SyncConnected)
            Console.WriteLine("已连接 ZK");
        return Task.CompletedTask;
    }
}
```

### 7.4 基础操作（async API）

```csharp
// 创建持久节点
string path = await zk.createAsync(
    "/app1/config",
    Encoding.UTF8.GetBytes("jdbc:mysql://..."),
    ZooDefs.Ids.OPEN_ACL_UNSAFE,
    CreateMode.PERSISTENT);

// 创建临时顺序节点(分布式锁核心)
string lockNode = await zk.createAsync(
    "/locks/order/lock_",
    Array.Empty<byte>(),
    ZooDefs.Ids.OPEN_ACL_UNSAFE,
    CreateMode.EPHEMERAL_SEQUENTIAL);

// 读取数据(返回 DataResult,含 .Data 和 .Stat)
DataResult result = await zk.getDataAsync("/app1/config", watch: false);
string value = Encoding.UTF8.GetString(result.Data);
long version = result.Stat.getVersion();              // ★ getter,不是 .Version
long ephemeralOwner = result.Stat.getEphemeralOwner();

// 获取子节点(返回 ChildrenResult,含 .Children 列表)
// ★ 重要: 只返回子节点名称(如 "lock_0000000001"),不是完整路径
ChildrenResult childrenResult = await zk.getChildrenAsync("/locks/order", watch: false);
List<string> childNames = childrenResult.Children;
// 如需完整路径要自己拼接: $"/locks/order/{name}"

// 修改数据(CAS 版本号,expectedVersion=-1 表示不校验)
await zk.setDataAsync("/app1/config", Encoding.UTF8.GetBytes("new value"), expectedVersion: -1);

// 删除节点(CAS 版本号)
await zk.deleteAsync("/app1/config", version: -1);

// 检查节点是否存在(可注册 Watch)
Stat stat = await zk.existsAsync("/app1/config", watch: false);
bool exists = stat != null;

// 获取会话信息
long sessionId = zk.getSessionId();           // ★ 方法,不是属性
ZooKeeperNetExZooKeeper.States state = zk.getState();  // ★ 方法
bool isConnected = state == ZooKeeperNetExZooKeeper.States.CONNECTED;
```

### 7.5 Watch 实现（重点：避免死锁）

```csharp
public class RealZooKeeperClient : IZooKeeperClient
{
    private readonly ZooKeeperNetExZooKeeper _zk;

    // ★ 踩坑: RegisterWatcher 必须用 fire-and-forget,不能 .GetAwaiter().GetResult()
    //   否则在 ZK IO 线程回调中同步等待 Task 会导致死锁(sync-over-async)
    public void RegisterWatcher(string path, Action<WatchEvent> callback)
    {
        lock (_watcherLock)
        {
            if (!_watchers.ContainsKey(path))
                _watchers[path] = new List<Action<WatchEvent>>();
            _watchers[path].Add(callback);
        }
        _ = RegisterWatcherAsync(path);  // fire-and-forget
    }

    private async Task RegisterWatcherAsync(string path)
    {
        try
        {
            // 用 ConfigureAwait(false) 避免 captured context
            await _zk.existsAsync(path, new InternalWatcher(path, OnWatchEvent))
                .ConfigureAwait(false);
        }
        catch { /* 忽略 */ }
    }

    // Watcher 是抽象类,方法名小写 process
    private class InternalWatcher : Watcher
    {
        private readonly string _path;
        private readonly Action<WatchedEvent> _callback;

        public InternalWatcher(string path, Action<WatchedEvent> callback)
        {
            _path = path;
            _callback = callback;
        }

        public override Task process(WatchedEvent @event)
        {
            _callback(@event);
            return Task.CompletedTask;
        }
    }
}
```

> **项目完整实现**：[RealZooKeeperClient.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/RealZooKeeperClient.cs)

---

## 8. 分布式锁深度剖析（重点）

> 本章是本文档的核心。配合 [ZkDistributedLock.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/ZkDistributedLock.cs) 学习。

### 8.1 为什么需要分布式锁

单机锁（`lock`/`Monitor`/`Mutex`）只能保证单进程内的互斥。当多个进程或多个机器需要访问**共享资源**时，需要跨进程的协调机制。

**支付系统典型场景**：
- 重复支付防护：用户重复点击"支付"，多个请求同时到达，必须只扣一次款
- 资金账户操作：对同一账户的扣款必须串行（避免并发读改写覆盖）
- 限流：每秒只允许 N 个请求进入
- 定时任务防重：多实例部署时只有一个实例执行定时任务

### 8.2 三种实现方式对比

| 方案 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| **数据库唯一索引** | 简单 | 性能低、有锁等待 | 资金强一致、低频 |
| **Redis SETNX** | 性能高（10x ZK） | 客户端崩溃需等过期、有死锁窗口 | 高并发缓存、限流 |
| **ZK 临时顺序节点** | CP 强一致、无死锁风险、自动释放 | 性能略低 | 资金强一致、关键互斥 |

**为什么支付资金场景优先用 ZK**：
- Redis 锁有"客户端崩溃未释放"问题：客户端崩溃后，锁需等 TTL 过期才释放（如 30s），期间无法操作
- ZK 锁基于会话：客户端崩溃 → 会话失效（默认 30s，可缩短）→ 临时节点立即删除 → 锁立即释放
- 资金操作宁可性能低一点，也不能有死锁窗口

### 8.3 ZK 分布式锁算法详解

#### 算法核心思想

```
所有客户端在 /lock 下创建临时顺序节点,序号最小者获得锁,其他客户端监听前一个节点
```

#### 完整步骤（5 步）

```
步骤 1: 创建临时顺序节点
   Client A → /lock/lock_00000001  (序号=1,最小)
   Client B → /lock/lock_00000002  (序号=2)
   Client C → /lock/lock_00000003  (序号=3)

步骤 2: 获取所有子节点,按序号排序
   children = [lock_00000001, lock_00000002, lock_00000003]

步骤 3: 检查自己是否最小
   A 检查 lock_00000001 == lock_00000001 ✓ → 获得锁,返回
   B 检查 lock_00000002 != lock_00000001 → 不是最小,继续
   C 检查 lock_00000003 != lock_00000001 → 不是最小,继续

步骤 4: 不是最小,监听前一个节点(避免惊群)
   B 监听 lock_00000001 (前一个)
   C 监听 lock_00000002 (前一个,不是 A)

步骤 5: 前一个节点删除,被唤醒,重新检查(可能多个节点同时唤醒)
   A 释放锁 → 删除 lock_00000001
   B 被 Watch 唤醒 → 重新检查 → 自己最小 → 获得锁
   B 释放锁 → 删除 lock_00000002
   C 被 Watch 唤醒 → 重新检查 → 自己最小 → 获得锁
```

#### 流程图

```
   Client A            Client B            Client C
      │                   │                   │
      │ create            │ create            │ create
      │ EPHEMERAL_SEQ     │ EPHEMERAL_SEQ     │ EPHEMERAL_SEQ
      ↓                   ↓                   ↓
  /lock_001           /lock_002           /lock_003
      │                   │                   │
      │ getChildren       │ getChildren       │ getChildren
      ↓                   ↓                   ↓
  [001,002,003]      [001,002,003]       [001,002,003]
      │                   │                   │
   最小? ✓             最小? ✗             最小? ✗
   获得锁              监听 lock_001       监听 lock_002
      │                   │                   │
   执行业务              等待...             等待...
      │                   │                   │
   释放锁                ↑                   │
   delete lock_001    Watch 触发             │
      │                   │                   │
                        getChildren          │
                        [002,003]            │
                        最小? ✓              │
                        获得锁               │
                        执行业务             │
                        释放锁               │
                        delete lock_002      │
                                              ↑
                                           Watch 触发
                                           getChildren
                                           [003]
                                           最小? ✓
                                           获得锁
```

### 8.4 羊群效应（Herd Effect）与公平锁

#### 什么是羊群效应

**错误做法**（所有客户端都监听同一个节点）：

```
所有客户端监听 /lock 节点的子节点变化

A 释放锁 → /lock 子节点变化
        → B、C、D、E... 全部被唤醒
        → 全部去争抢锁
        → 只有 B 获得,C/D/E 又重新等待
        → ZK 服务器承受大量 Watch 通知压力
```

**问题**：
- N 个客户端争锁时，一次释放会唤醒 N-1 个客户端
- ZK 服务器压力 O(N)，客户端无效争抢
- 网络风暴、CPU 抖动

#### 正确做法（链式监听）

```
A 释放锁 → 只通知 B(B 监听了 A 的节点)
        → B 获得锁
        → B 释放锁 → 只通知 C(C 监听了 B 的节点)
        → C 获得锁
```

**优势**：
- 每次只有 1 个客户端被唤醒
- ZK 服务器压力 O(1)
- FIFO 公平锁（先到先得）

#### 公平锁 vs 非公平锁

| 类型 | 实现 | 特性 |
|------|------|------|
| **公平锁（FIFO）** | ZK 顺序节点 + 链式监听 | 先请求先获得，无饥饿 |
| **非公平锁** | Redis SETNX + 重试 | 谁抢到谁得，可能饥饿 |

> **★ 本项目场景 3 即演示公平锁**：3 个客户端依次请求，按请求顺序依次获锁。

### 8.5 CAS 乐观锁与版本号

ZK 的每个 ZNode 都有 `version` 字段，每次 `setData`/`delete` 可指定 `expectedVersion` 进行 CAS 校验。

```csharp
// 读取当前版本
var data = await zk.getDataAsync("/config", false);
long currentVersion = data.Stat.getVersion();  // 如 5

// 修改时指定版本号(CAS)
try
{
    await zk.setDataAsync("/config", newData, expectedVersion: 5);
    // 成功
}
catch (KeeperException.BadVersionException)
{
    // 版本不匹配,说明其他客户端已修改
    // 重试或放弃
}
```

**分布式锁中 CAS 的应用**：
- 释放锁时，删除节点可指定版本号，防止误删他人创建的节点
- 配置更新时，CAS 避免覆盖他人修改

### 8.6 可重入锁

**问题**：当前实现不可重入——同一线程/进程再次获取锁会创建第二个节点，导致自死锁。

**解决方案**：在节点数据中记录 owner 标识 + 重入计数

```csharp
public class ReentrantDistributedLock
{
    private readonly ZooKeeper _zk;
    private readonly string _lockPath;
    private readonly string _ownerId;  // GUID 标识当前进程/线程
    private int _reentrantCount;
    private string _myNode;

    public async Task<bool> TryAcquireAsync()
    {
        // 1. 检查是否已持有(可重入)
        if (_myNode != null)
        {
            var data = await _zk.getDataAsync(_myNode, false);
            var ownerInfo = ParseOwner(data.Data);
            if (ownerInfo.OwnerId == _ownerId)
            {
                _reentrantCount++;
                // 更新重入计数
                await _zk.setDataAsync(_myNode, Serialize(_ownerId, _reentrantCount), -1);
                return true;
            }
        }

        // 2. 首次获取,创建临时顺序节点,数据中记录 owner
        _myNode = await _zk.createAsync(
            $"{_lockPath}/lock_",
            Serialize(_ownerId, 1),
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
        _reentrantCount = 1;

        // 3. 检查最小 + 链式监听
        return await CheckAndAcquireAsync();
    }
}
```

> **★ 注意**：本项目 Demo 实现的是**不可重入锁**，简化演示。生产环境如 Curator 的 `InterProcessMutex` 是可重入的。

### 8.7 锁释放与宕机处理

#### 主动释放

```csharp
public void Dispose()
{
    if (_acquired && _myNode != null)
    {
        try
        {
            // 删除自己的节点 → 触发下一个客户端的 Watch
            _zk.deleteAsync(_myNode, version: -1).Wait();
            Console.WriteLine($"[Lock] 释放锁: {_myNode}");
        }
        catch { /* 忽略 */ }
    }
}
```

#### 客户端宕机（被动释放）

```
1. 客户端进程崩溃 / 机器宕机 / 网络断开
2. ZK 检测到心跳超时(sessionTimeout,默认 30s,可缩短到 5s)
3. ZK 服务端使会话失效
4. 该会话创建的所有临时节点自动删除
5. 下一个客户端的 Watch 被触发 → 获得锁
```

**关键特性**：
- 不会死锁：客户端宕机后，锁会话内自动释放
- 不会泄漏：临时节点与 session 绑定，session 关闭即清理
- 不会误删：只删自己 session 的临时节点，不影响其他客户端

### 8.8 完整 C# 实现（指向项目代码）

本项目提供两种实现：

**InMemory 模拟版**：[InMemoryZooKeeper.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/InMemoryZooKeeper.cs)
- 用 static 字段模拟 ZK 服务端的共享 ZNode 树
- 让多个客户端实例共享存储，正确模拟分布式行为
- 适合无 ZK 服务时学习算法

**Real 真实连接版**：[RealZooKeeperClient.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/RealZooKeeperClient.cs)
- 适配 `ZooKeeperNetEx` 真实客户端
- 通过 `org.apache.zookeeper` API 操作真实 ZK 服务
- 适合学习真实 API 和踩坑

**锁算法实现**：[ZkDistributedLock.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/ZkDistributedLock.cs)

```csharp
public async Task<bool> TryAcquireAsync(TimeSpan timeout)
{
    // 1. 确保锁根路径存在
    await EnsurePathAsync(_lockPath);

    // 2. 创建临时顺序节点(★关键: EPHEMERAL_SEQUENTIAL)
    _myNode = await _zk.CreateAsync(
        $"{_lockPath}/lock_",
        data: Array.Empty<byte>(),
        type: ZNodeType.EphemeralSequential);

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

private async Task<bool> CheckAndAcquireAsync()
{
    // 获取所有子节点,按序号排序
    var children = await _zk.GetChildrenAsync(_lockPath);
    var sorted = children
        .OrderBy(c => c.Substring(c.LastIndexOf('/') + 1))
        .ToList();

    // 序号最小者获锁
    var minNode = sorted[0];
    if (_myNode == minNode)
    {
        _acquired = true;
        return true;
    }

    // 不是最小 → 监听前一个节点(避免惊群)
    var myIndex = sorted.IndexOf(_myNode!);
    var previousNode = sorted[myIndex - 1];
    return false;
}
```

### 8.9 性能与限制

| 指标 | 数值 |
|------|------|
| 单次创建节点延迟 | 1-5ms（取决于网络） |
| 单 ZK 集群 QPS | 5000-10000（读）、500-1000（写） |
| 单节点数据上限 | 1MB |
| Watch 触发延迟 | 10-100ms（网络往返） |

**性能瓶颈**：
- 所有写请求都经过 Leader，Leader 是单点瓶颈
- Watch 触发有网络延迟，不适合超高实时场景
- 临时节点清理有 sessionTimeout 延迟

**优化建议**：
- 短 sessionTimeout：5-10s 加速宕机检测
- 合理的 ZNode 路径深度：避免过深影响查找性能
- 分片锁路径：`/locks/order-{shardId}` 避免单节点争抢

### 8.10 常见陷阱

#### 陷阱 1：Watch 一次性导致漏通知

```
A 监听 lock_001 删除事件
A 收到通知,准备检查自己
但在 A 重新检查前,B 又创建了 lock_001.5 并删除了
此时 A 应该是新的最小,但没有 Watch 通知它
```

**解决**：Watch 触发后必须重新检查子节点列表（不能只靠 Watch 推断）。

#### 陷阱 2：sync-over-async 死锁

```csharp
// ★ 错误: 在 ZK IO 线程回调中同步等待 Task,导致死锁
zk.RegisterWatcher(path, callback => {
    zk.existsAsync(path, watcher).GetAwaiter().GetResult();  // 死锁!
});

// ★ 正确: 用 fire-and-forget + ConfigureAwait(false)
zk.RegisterWatcher(path, callback => {
    _ = RegisterWatcherAsync(path);  // fire-and-forget
});
private async Task RegisterWatcherAsync(string path)
    => await _zk.existsAsync(path, watcher).ConfigureAwait(false);
```

#### 陷阱 3：getChildren 返回相对路径

```csharp
// ★ 错误: 以为 getChildrenAsync 返回完整路径
var children = await zk.getChildrenAsync("/locks/order", false);
// children = ["lock_0000000001", "lock_0000000002"]  // 相对名称!
await zk.existsAsync(children[0]);  // 错误: "/lock_0000000001" 不存在

// ★ 正确: 需要手动拼接完整路径
var fullPaths = children.Select(c => $"/locks/order/{c}").ToList();
```

#### 陷阱 4：临时节点清理延迟

```
客户端 A 宕机 → ZK 在 sessionTimeout 后才清理临时节点
  - 默认 sessionTimeout = 30s
  - 期间 B 仍在等 A 释放锁(其实 A 已死)

优化: 用短 sessionTimeout(如 5s)加速清理
```

---

## 9. 运维要点

### 9.1 部署

```bash
# Docker 部署单机版(学习用)
docker run -d --name zookeeper -p 2181:2181 \
    -e ZOO_MY_ID=1 \
    confluentinc/cp-zookeeper:latest

# Docker Compose 集群版(3 节点)
# docker-compose.yml
version: '3'
services:
  zoo1:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOO_MY_ID: 1
      ZOO_SERVERS: server.1=0.0.0.0:2888:3888;server.2=zoo2:2888:3888;server.3=zoo3:2888:3888
    ports: ["2181:2181"]

  zoo2:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOO_MY_ID: 2
      ZOO_SERVERS: server.1=zoo1:2888:3888;server.2=0.0.0.0:2888:3888;server.3=zoo3:2888:3888
    ports: ["2182:2181"]

  zoo3:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOO_MY_ID: 3
      ZOO_SERVERS: server.1=zoo1:2888:3888;server.2=zoo2:2888:3888;server.3=0.0.0.0:2888:3888
    ports: ["2183:2181"]
```

### 9.2 关键配置（zoo.cfg）

```properties
tickTime=2000                  # 基本时间单位(ms),心跳/超时倍数
initLimit=10                   # Follower 初始连接 Leader 超时(10 tick = 20s)
syncLimit=5                    # 请求/响应超时(5 tick = 10s)
dataDir=/var/lib/zookeeper     # 数据持久化目录
clientPort=2181                # 客户端端口
maxClientCnxns=60              # 单 IP 最大连接数
autopurge.snapRetainCount=3    # 保留快照数
autopurge.purgeInterval=1      # 自动清理间隔(小时)

# 集群节点配置
server.1=zoo1:2888:3888        # 主机名:数据同步端口:选举端口
server.2=zoo2:2888:3888
server.3=zoo3:2888:3888
```

### 9.3 四字命令

ZK 提供 4 字母的诊断命令（通过 `echo` + `nc`）：

```bash
echo stat | nc localhost 2181      # 集群状态
echo ruok | nc localhost 2181      # 是否健康(返回 imok)
echo mntr | nc localhost 2181      # 监控指标(Prometheus 格式)
echo dump | nc localhost 2181     # 会话和临时节点
echo conf | nc localhost 2181     # 配置信息
echo cons | nc localhost 2181     # 连接详情
```

### 9.4 数据快照 + 事务日志

```
/var/lib/zookeeper/
├── version-2/
│   ├── log.1                    # 事务日志
│   ├── log.2
│   ├── snapshot.100              # 数据快照
│   └── snapshot.200
└── myid                          # 节点 ID(对应 server.N)
```

- **事务日志**：所有写操作顺序追加
- **快照**：定时全量内存数据
- **恢复**：加载快照 → 重放事务日志

---

## 10. Real 模式连接与踩坑记录

### 10.1 Docker 启动 ZK

本项目测试使用 `confluentinc/cp-zookeeper:7.5.0`：

```bash
docker run -d --name payment-zookeeper \
    -p 2181:2181 \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:7.5.0
```

### 10.2 运行 Real 模式

```bash
# 默认连接 127.0.0.1:2181
dotnet run --project examples/ZooKeeperDemo -- real

# 显式指定地址
dotnet run --project examples/ZooKeeperDemo -- real 127.0.0.1:2181
```

### 10.3 踩坑记录（实测总结）

#### 踩坑 1：命名空间错误

**现象**：编译报错 `找不到 ZooKeeperNet 命名空间`

**原因**：网上多数教程用的是老的 `ZooKeeperNet` 库，但本项目用 `ZooKeeperNetEx`，命名空间是 `org.apache.zookeeper`（与 Java 客户端一致）

**解决**：
```csharp
// ★ 错误
using ZooKeeperNet;
var zk = new ZooKeeper("127.0.0.1:2181", ...);

// ★ 正确
using org.apache.zookeeper;
using ZooKeeperNetExZooKeeper = org.apache.zookeeper.ZooKeeper;
var zk = new ZooKeeperNetExZooKeeper("127.0.0.1:2181", ...);
```

#### 踩坑 2：Watcher 是抽象类不是接口

**现象**：`public class MyWatcher : IWatcher` 报错

**原因**：`ZooKeeperNetEx` 的 `Watcher` 是抽象类，方法名小写 `process`

**解决**：
```csharp
// ★ 错误
public class MyWatcher : IWatcher
{
    public void Process(WatchedEvent @event) { ... }
}

// ★ 正确
public class MyWatcher : Watcher
{
    public override Task process(WatchedEvent @event) { ... }
}
```

#### 踩坑 3：API 全是 async

**现象**：`zk.Create()` 不存在

**原因**：`ZooKeeperNetEx` 所有方法都是 async 版本，无同步版本

**解决**：
```csharp
// ★ 错误
string path = zk.Create("/lock", data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

// ★ 正确
string path = await zk.createAsync("/lock", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
```

#### 踩坑 4：Stat 字段不可访问

**现象**：`stat.Version` 编译报错

**原因**：`Stat` 的字段是非 public 的，必须用 Java 风格 getter

**解决**：
```csharp
// ★ 错误
long version = stat.Version;
long ephemeralOwner = stat.EphemeralOwner;

// ★ 正确
long version = stat.getVersion();
long ephemeralOwner = stat.getEphemeralOwner();
```

> **排查方法**：用 PowerShell 反射查询 DLL 的所有类型和成员：
> ```powershell
> [Reflection.Assembly]::LoadFrom("path/to/ZooKeeperNetEx.dll").GetTypes()
> ```

#### 踩坑 5：getChildren 返回相对路径

**现象**：用 `getChildrenAsync` 返回的字符串去 `existsAsync`，发现节点不存在

**原因**：真实 ZK 的 `getChildrenAsync` 只返回子节点**名称**（如 `lock_0000000001`），不是完整路径

**解决**：
```csharp
// ★ 错误
var children = await zk.getChildrenAsync("/locks/order", false);
await zk.existsAsync(children[0]);  // 错误: "/lock_0000000001" 不存在

// ★ 正确
var fullPaths = children.Select(c => $"/locks/order/{c}").ToList();
```

#### 踩坑 6：sync-over-async 死锁

**现象**：注册 Watch 后程序卡死

**原因**：在 `RegisterWatcher` 中用 `existsAsync(...).GetAwaiter().GetResult()` 同步等待，在 ZK IO 线程回调中导致死锁

**解决**：用 fire-and-forget + `ConfigureAwait(false)`：
```csharp
// ★ 错误
public void RegisterWatcher(string path, Action<WatchEvent> callback)
{
    _zk.existsAsync(path, watcher).GetAwaiter().GetResult();  // 死锁!
}

// ★ 正确
public void RegisterWatcher(string path, Action<WatchEvent> callback)
{
    _ = RegisterWatcherAsync(path);  // fire-and-forget
}
private async Task RegisterWatcherAsync(string path)
    => await _zk.existsAsync(path, watcher).ConfigureAwait(false);
```

#### 踩坑 7：临时节点清理延迟

**现象**：客户端 `Dispose()` 后立即检查，临时节点还在

**原因**：Real ZK 的临时节点清理有 `sessionTimeout` 延迟
- 客户端 `closeAsync` 只是关闭会话
- ZK 服务端在 `sessionTimeout` 后才清理临时节点

**解决**：用短 `sessionTimeout`（如 5s）加速演示，或循环检查清理完成：
```csharp
var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
while (DateTime.UtcNow < deadline)
{
    var remaining = await zk3.GetChildrenAsync("/services/payment-service");
    if (!remaining.Any()) break;
    await Task.Delay(1000);
}
```

### 10.4 InMemory vs Real 对比

| 维度 | InMemory 模式 | Real 模式 |
|------|--------------|----------|
| 启动依赖 | 无 | Docker 启动 ZK |
| 数据存储 | 进程内 static 字段 | ZK 服务端内存 |
| 共享性 | 多个实例共享 static | 多个客户端连同一 ZK |
| 临时节点清理 | `SimulateSessionExpired()` 立即 | 等 `sessionTimeout` |
| Watch 触发 | 同步 | 网络往返（异步） |
| 用途 | 学习算法 | 学习真实 API |

> **★ InMemory 用 static 共享存储是关键**：让多个 `InMemoryZooKeeper` 实例共享同一份 ZNode 树，才能正确演示分布式锁/选举等需要多客户端协作的场景。

---

## 11. 常见问题

### 11.1 脑裂（Split-Brain）

**问题**：网络分区导致集群分裂为多个子集群，每个选自己 Leader，产生多个"主"。

**ZK 解决**：
- 过半同意才生效（Quorum）
- 不满足过半的分区无法完成写操作
- 自动避免脑裂

### 11.2 Watch 一次性问题

**问题**：ZK 3.x 的 Watch 触发后失效，重新注册期间的数据变化可能丢失。

**解决**：
- 客户端记录上次版本号，重新注册时对比
- 升级 ZK 3.6+，使用持久 Watch
- 使用 Curator 的 `Cache` 封装

### 11.3 数据大小限制

**限制**：单个 ZNode 数据 ≤ 1MB。

**原因**：ZK 是协调服务，不是数据库。大数据应存外部存储，ZK 只存引用。

---

## 12. 面试话术（12 道 Q&A）

### Q1: ZooKeeper 是什么？

> ZooKeeper 是 Apache 的分布式协调服务，为分布式应用提供配置中心、注册中心、分布式锁、Leader 选举能力。
>
> 数据模型是树形 ZNode，每个节点可存数据（≤ 1MB）和子节点。基于 ZAB 协议实现强一致性（CP），适合需要可靠协调的场景。

### Q2: ZK 的 ZAB 协议和 Raft 有什么区别？

> 两者都是**强一致性**算法，核心思想类似（Leader + 过半同意）：
> - **ZAB**：ZK 专用，分"Leader 选举"和"原子广播"两阶段，ZXID 单调递增保证事务顺序
> - **Raft**：通用算法，分 Leader 选举、日志复制、安全性三部分，更易理解
>
> 实际工程差异不大，ZAB 是 Raft 之前的设计，Raft 是 ZAB 的简化教学版本。

### Q3: ZK 为什么用奇数节点？

> 因为 ZAB 协议需要**过半同意**才生效。
> - 3 节点：容忍 1 故障（需 2 票过半）
> - 4 节点：仍只容忍 1 故障（需 3 票过半，无优势）
> - 5 节点：容忍 2 故障
>
> 所以偶数节点浪费资源，奇数性价比最高。

### Q4: 分布式锁用 ZK 还是 Redis？（★重点）

> 各有优势：
> - **ZK 锁**：CP 强一致，会话失效自动释放，**无死锁风险**；性能略低
> - **Redis 锁**：AP 高性能（10x 于 ZK），但需设置过期时间，**存在客户端崩溃未释放**的问题
>
> 资金类强一致场景用 ZK；高并发缓存场景用 Redis。
>
> 我们支付系统的分布式锁优先用 Redis（性能），关键资金操作用 ZK（安全）。

### Q5: 详细讲讲 ZK 分布式锁的实现原理（★重点）

> 算法核心：所有客户端在 `/lock` 下创建**临时顺序节点**，序号最小者获得锁，其他客户端监听前一个节点。
>
> 完整步骤：
> 1. 创建临时顺序节点 `/lock/lock_0000000X`
> 2. 获取所有子节点，按序号排序
> 3. 自己是序号最小 → 获得锁
> 4. 不是最小 → 监听**前一个节点**（不是所有节点，避免惊群）
> 5. 前一个节点删除 → Watch 唤醒 → 重新检查
>
> 关键设计：
> - **临时节点**：客户端宕机 → 会话失效 → 节点自动删 → 锁自动释放（防死锁）
> - **顺序节点**：决定获取锁顺序（FIFO 公平锁）
> - **链式监听**：每个客户端只监听前一个节点，避免羊群效应

### Q6: 什么是羊群效应？如何避免？（★重点）

> **羊群效应**：如果所有客户端都监听 `/lock` 节点的子节点变化，一次锁释放会唤醒所有 N 个等待的客户端，但只有 1 个能获得锁，其余 N-1 个又被阻塞，浪费资源。
>
> **避免方案**：链式监听 —— 每个客户端只监听**前一个节点**的删除事件。
> - A 释放锁 → 只通知 B（B 监听 A）→ B 获得锁
> - B 释放锁 → 只通知 C（C 监听 B）→ C 获得锁
> - 每次只有 1 个客户端被唤醒，ZK 服务器压力 O(1)

### Q7: ZK 的临时节点和持久节点有什么区别？（★重点）

> - **持久节点**：客户端断开后仍存在，需显式删除
> - **临时节点**：客户端会话失效时自动删除
>
> 临时节点是分布式锁、服务注册的核心：客户端宕机 → 会话失效 → 节点自动删除 → 自动释放锁/下线服务，避免死锁和脏数据。
>
> **生命周期**：临时节点的生命周期与 session 绑定，session 关闭即清理。注意不是与客户端对象绑定，调用 `close()` 也会触发清理。

### Q8: ZK 的顺序节点有什么用？（★重点）

> 顺序节点在创建时自动追加单调递增序号（如 `lock_00000001`），核心场景：
> 1. **分布式锁**：所有客户端创建顺序节点，序号最小者获锁，避免惊群
> 2. **Leader 选举**：序号最小者当选主
> 3. **FIFO 队列**：通过顺序节点模拟先进先出队列
>
> **关键特性**：序号是 ZK 服务端生成的，全局单调递增，避免客户端时钟不同步问题。

### Q9: ZK 的 Watch 机制为什么说"一次性"？

> ZK 3.x 的 Watch 触发后即失效，需要客户端重新注册才能再次监听。
> 设计原因：避免 Watcher 丢失通知时无限重试。
> 缺点：重新注册期间的数据变化可能丢失。
>
> ZK 3.6+ 引入持久 Watch（PersistentWatcher），解决了这个问题。或者用 Curator 的 PathChildrenCache / TreeCache 封装。

### Q10: ZK 如何实现 Leader 选举？

> 经典模式：
> 1. 所有节点在 `/election` 下创建临时顺序节点
> 2. 序号最小的成为 Leader
> 3. 其他节点监听前一个节点
> 4. Leader 宕机 → 节点删除 → 下一个最小者被唤醒 → 新 Leader
>
> 链式监听避免惊群，临时节点保证宕机自动切换。

### Q11: ZK 集群挂了 Leader 怎么办？

> Leader 宕机后：
> 1. Follower 检测到心跳超时（默认 2 × tickTime × syncLimit = 20s）
> 2. 进入 LOOKING 状态，发起 Leader 选举
> 3. 各 Follower 交换 ZXID，最大的当选（最新数据）
> 4. 新 Leader 上线，恢复服务
>
> 选举期间集群不可写，但读可用（最终一致）。

### Q12: ZK 和 Kafka 是什么关系？

> Kafka 早期版本（0.9 之前）**完全依赖 ZK**：
> - Broker 注册到 ZK
> - Topic/Partition 元数据存 ZK
> - Consumer Group 消费进度存 ZK
> - Controller 选举依赖 ZK
>
> Kafka 新版（2.8+）通过 **KRaft** 模式移除 ZK 依赖，用 Raft 协议自己实现元数据一致性，简化部署。
>
> 我们学 ZK 一个重要原因就是理解 Kafka 早期架构。

---

## 13. 学习路径

```
入门(1-2 天):
├─ 1. Docker 启动单机 ZK
├─ 2. zkCli.sh 命令行操作
│   - create /path data
│   - get /path
│   - ls /path
│   - delete /path
└─ 3. 跑本项目 examples/ZooKeeperDemo(先 InMemory 模式)

进阶(3-5 天):
├─ 1. 学习 ZAB 协议(本文第 3 章)
├─ 2. 精读分布式锁原理(本文第 8 章,★重点)
│   - 配合 ZkDistributedLock.cs 看代码
├─ 3. 实现 Leader 选举
└─ 4. Real 模式连接真实 ZK(本文第 10 章)

实战(1-2 周):
├─ 1. 用 ZK 实现服务注册发现(参考场景 5)
├─ 2. 用 ZK 实现配置中心(参考场景 2)
├─ 3. 研究 Kafka ZK 模式 vs KRaft 模式差异
└─ 4. 对比 Redis 分布式锁的实现差异
```

---

## 14. 参考资源

- [ZooKeeper 官网](https://zookeeper.apache.org/)
- [ZooKeeper 文档](https://zookeeper.apache.org/doc/current/)
- [ZooKeeperNetEx (C# 客户端)](https://github.com/mmarinero/ZooKeeperNetEx)
- [Curator (Java 高级封装)](https://curator.apache.org/)
- 本项目代码：
  - [ZooKeeperDemo Program.cs](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/Program.cs)
  - [IZooKeeperClient.cs 接口](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/IZooKeeperClient.cs)
  - [InMemoryZooKeeper.cs 内存模拟实现](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/InMemoryZooKeeper.cs)
  - [RealZooKeeperClient.cs 真实 ZK 适配器](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/RealZooKeeperClient.cs)
  - [ZkDistributedLock.cs 分布式锁实现](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/ZkDistributedLock.cs)
  - [ZkLeaderElection.cs Leader 选举实现](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/ZkLeaderElection.cs)
