# ZooKeeper 专项学习文档

> 面向 .NET 工程师的 ZooKeeper 深度学习，含 C# 实战 Demo。

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
│  3. 分布式锁   - 跨进程互斥                 │
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

| 类型 | 说明 | 创建方式 |
|------|------|---------|
| **持久节点（PERSISTENT）** | 客户端断开后仍存在 | `create /path data` |
| **持久顺序节点（PERSISTENT_SEQUENTIAL）** | 持久 + 自动追加单调递增序号 | `create -s /path data` |
| **临时节点（EPHEMERAL）** | 客户端会话失效自动删除 | `create -e /path data` |
| **临时顺序节点（EPHEMERAL_SEQUENTIAL）** | 临时 + 顺序（分布式锁核心） | `create -e -s /path data` |

### 2.3 ZNode 数据结构

每个 ZNode 包含：

| 字段 | 说明 |
|------|------|
| `data` | 节点存储的数据（≤ 1MB） |
| `czxid` | 创建事务 ID |
| `mzxid` | 最后修改事务 ID |
| `version` | 数据版本（CAS 乐观锁） |
| `cversion` | 子节点版本 |
| `ephemeralOwner` | 临时节点持有者会话 ID |
| `dataLength` | 数据长度 |
| `numChildren` | 子节点数量 |

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

  Leader: 处理所有写请求,提案给 Follower
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
// 注册 Watch
client.GetData("/config", watch: true);

// Watch 触发后失效,需要重新注册
client.DataChanged += (s, e) =>
{
    Console.WriteLine($"数据变化: {e.Data}");
    // ⚠ 重新注册(并发可能漏通知)
    client.GetData("/config", watch: true);
};
```

**问题**：重新注册期间的数据变化可能丢失。

### 4.2 持久 Watch（ZK 3.6+）

```csharp
// 注册一次,持续监听
client.AddWatch("/config", watcher, WatcherMode.Persistent);
```

### 4.3 三种 Watch 类型

| 类型 | 触发条件 |
|------|---------|
| **GetData Watch** | 节点数据变化或被删除 |
| **GetChildren Watch** | 子节点列表变化（新增/删除） |
| **Exists Watch** | 节点被创建/删除/数据变化 |

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
- `world:anyone` - 任何人（默认）
- `auth:user:pwd` - 用户密码认证
- `digest` - SHA1 摘要认证

---

## 6. 典型应用模式

### 6.1 分布式锁（最常用）

**核心思想**：所有客户端在 `/lock` 下创建**临时顺序节点**，序号最小者获得锁。

```
步骤:
1. 所有客户端在 /lock 下创建临时顺序节点
   /lock/lock_00000001  ← 客户端 A
   /lock/lock_00000002  ← 客户端 B
   /lock/lock_00000003  ← 客户端 C

2. 每个客户端检查自己是否序号最小
   A 是最小 → 获得锁
   B/C 监听前一个节点(序号-1)

3. A 释放锁(会话结束节点自动删)
   → B 收到通知,检查自己是新的最小 → 获得锁

4. 链式唤醒,避免惊群
```

**为什么用临时节点？**
- 客户端宕机 → 会话失效 → 节点自动删 → 释放锁
- 避免死锁

**为什么用顺序节点？**
- 决定获取锁顺序（FIFO）
- 监听前一个节点，避免惊群

### 6.2 Leader 选举

```
步骤:
1. 所有节点创建临时节点 /election/node_xxx
2. 检查自己是否最小节点
3. 最小者成为 Leader,其他监听前一个节点
4. Leader 宕机 → 节点删除 → 下一个被唤醒 → 成为新 Leader
```

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

---

## 7. C# 客户端使用

### 7.1 主流库对比

| 库 | 说明 |
|----|------|
| **ZooKeeperNet** | 老牌（Apache 官方 Java 移植），维护缓慢 |
| **ZooKeeperNetEx** | ZooKeeperNet 的扩展分支，社区维护 |
| **curator-zookeeper** | Netflix Curator 风格的封装（Java 移植） |
| **DotNetty + 自实现** | 性能好但开发量大 |

**推荐**：ZooKeeperNetEx（NuGet: `Puppet.ZooKeeper` 或 `ZooKeeperNetEx`）

### 7.2 基础操作

```csharp
using ZooKeeperNet;

// 1. 连接 ZK
using var zk = new ZooKeeper("127.0.0.1:2181", TimeSpan.FromSeconds(30), new Watcher());

// 2. 创建持久节点
zk.Create("/config", Encoding.UTF8.GetBytes("hello"),
    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

// 3. 读取数据
byte[] data = zk.GetData("/config", watch: false, null);
string value = Encoding.UTF8.GetString(data);

// 4. 创建临时顺序节点
string path = zk.Create("/lock/lock_", Guid.NewGuid().ToByteArray(),
    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

// 5. 获取子节点
IEnumerable<string> children = zk.GetChildren("/lock", watch: false);

// 6. 删除节点(需指定版本号,CAS)
zk.Delete("/config", version: 0);
```

### 7.3 Watch 实现

```csharp
public class ConfigWatcher : IWatcher
{
    private readonly ZooKeeper _zk;

    public ConfigWatcher(ZooKeeper zk) { _zk = zk; }

    public void Process(WatchedEvent @event)
    {
        if (@event.Type == EventType.NodeDataChanged)
        {
            // 重新读取
            byte[] newData = _zk.GetData("/config", watch: true, null);
            Console.WriteLine($"配置变化: {Encoding.UTF8.GetString(newData)}");
            // ⚠ 一次性 Watch,需重新注册
        }
    }
}

// 注册
zk.GetData("/config", watch: true, null);
```

### 7.4 分布式锁实现

```csharp
public class DistributedLock
{
    private readonly ZooKeeper _zk;
    private readonly string _lockPath;
    private string _myNode;

    public DistributedLock(ZooKeeper zk, string lockPath)
    {
        _zk = zk;
        _lockPath = lockPath;
    }

    public async Task<bool> TryAcquireAsync()
    {
        // 1. 创建临时顺序节点
        _myNode = _zk.Create($"{_lockPath}/lock-", new byte[0],
            Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // 2. 获取所有子节点,排序
        var children = _zk.GetChildren(_lockPath, false)
            .OrderBy(n => n)
            .ToList();

        // 3. 检查自己是否最小
        var myName = _myNode.Substring(_lockPath.Length + 1);
        if (myName == children[0])
            return true;  // 获得锁

        // 4. 不是最小 → 监听前一个节点
        var myIndex = children.IndexOf(myName);
        var prevNode = children[myIndex - 1];

        var wait = new ManualResetEventSlim();
        _zk.Exists($"{_lockPath}/{prevNode}", new DeleteWatcher(wait));

        // 等待前一个节点删除(或超时)
        return wait.Wait(TimeSpan.FromSeconds(30));
    }

    public void Release()
    {
        if (_myNode != null)
            _zk.Delete(_myNode, -1);
    }
}
```

---

## 8. 运维要点

### 8.1 部署

```bash
# Docker 部署单机版(学习用)
docker run -d --name zookeeper -p 2181:2181 \
    -e ZOO_MY_ID=1 \
    confluentinc/cp-zookeeper:latest

# Docker Compose 集群版
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

### 8.2 关键配置（zoo.cfg）

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

### 8.3 四字命令

ZK 提供 4 字母的诊断命令（通过 `echo` + `nc`）：

```bash
echo stat | nc localhost 2181      # 集群状态
echo ruok | nc localhost 2181      # 是否健康(返回 imok)
echo mntr | nc localhost 2181      # 监控指标(Prometheus 格式)
echo dump | nc localhost 2181     # 会话和临时节点
echo conf | nc localhost 2181     # 配置信息
echo cons | nc localhost 2181     # 连接详情
```

### 8.4 数据快照 + 事务日志

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

## 9. 常见问题

### 9.1 脑裂（Split-Brain）

**问题**：网络分区导致集群分裂为多个子集群，每个选自己 Leader，产生多个"主"。

**ZK 解决**：
- 过半同意才生效（Quorum）
- 不满足过半的分区无法完成写操作
- 自动避免脑裂

### 9.2 Watch 一次性问题

**问题**：ZK 3.x 的 Watch 触发后失效，重新注册期间的数据变化可能丢失。

**解决**：
- 客户端记录上次版本号，重新注册时对比
- 升级 ZK 3.6+，使用持久 Watch
- 使用 Curator 的 `Cache` 封装

### 9.3 数据大小限制

**限制**：单个 ZNode 数据 ≤ 1MB。

**原因**：ZK 是协调服务，不是数据库。大数据应存外部存储，ZK 只存引用。

---

## 10. 面试话术（10 道 Q&A）

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

### Q4: 分布式锁用 ZK 还是 Redis？

> 各有优势：
> - **ZK 锁**：CP 强一致，会话失效自动释放，无死锁风险；性能略低
> - **Redis 锁**：AP 高性能（10x 于 ZK），但需设置过期时间，存在客户端崩溃未释放的问题
>
> 资金类强一致场景用 ZK；高并发缓存场景用 Redis。我们支付系统的分布式锁优先用 Redis（性能），关键资金操作用 ZK（安全）。

### Q5: ZK 的临时节点和持久节点有什么区别？

> - **持久节点**：客户端断开后仍存在，需显式删除
> - **临时节点**：客户端会话失效时自动删除
>
> 临时节点是分布式锁、服务注册的核心：客户端宕机 → 会话失效 → 节点自动删除 → 自动释放锁/下线服务，避免死锁和脏数据。

### Q6: ZK 的顺序节点有什么用？

> 顺序节点在创建时自动追加单调递增序号（如 `lock_00000001`），核心场景：
> 1. **分布式锁**：所有客户端创建顺序节点，序号最小者获锁，避免惊群
> 2. **Leader 选举**：序号最小者当选主
> 3. **FIFO 队列**：通过顺序节点模拟先进先出队列

### Q7: ZK 的 Watch 机制为什么说"一次性"？

> ZK 3.x 的 Watch 触发后即失效，需要客户端重新注册才能再次监听。
> 设计原因：避免 Watcher 丢失通知时无限重试。
> 缺点：重新注册期间的数据变化可能丢失。
>
> ZK 3.6+ 引入持久 Watch（PersistentWatcher），解决了这个问题。或者用 Curator 的 PathChildrenCache / TreeCache 封装。

### Q8: ZK 如何实现 Leader 选举？

> 经典模式：
> 1. 所有节点在 `/election` 下创建临时顺序节点
> 2. 序号最小的成为 Leader
> 3. 其他节点监听前一个节点
> 4. Leader 宕机 → 节点删除 → 下一个最小者被唤醒 → 新 Leader
>
> 链式监听避免惊群，临时节点保证宕机自动切换。

### Q9: ZK 集群挂了 Leader 怎么办？

> Leader 宕机后：
> 1. Follower 检测到心跳超时（默认 2 × tickTime × syncLimit = 20s）
> 2. 进入 LOOKING 状态，发起 Leader 选举
> 3. 各 Follower 交换 ZXID，最大的当选（最新数据）
> 4. 新 Leader 上线，恢复服务
>
> 选举期间集群不可写，但读可用（最终一致）。

### Q10: ZK 和 Kafka 是什么关系？

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

## 11. 学习路径

```
入门(1-2 天):
├─ 1. Docker 启动单机 ZK
├─ 2. zkCli.sh 命令行操作
│   - create /path data
│   - get /path
│   - ls /path
│   - delete /path
└─ 3. 跑本项目 examples/ZooKeeperDemo

进阶(3-5 天):
├─ 1. 学习 ZAB 协议
├─ 2. 实现分布式锁
├─ 3. 实现 Leader 选举
└─ 4. Docker Compose 启动 3 节点集群

实战(1-2 周):
├─ 1. 用 ZK 实现服务注册发现
├─ 2. 用 ZK 实现配置中心
└─ 3. 研究 Kafka ZK 模式 vs KRaft 模式差异
```

---

## 12. 参考资源

- [ZooKeeper 官网](https://zookeeper.apache.org/)
- [ZooKeeper 文档](https://zookeeper.apache.org/doc/current/)
- [ZooKeeperNetEx (C# 客户端)](https://github.com/mmarinero/ZooKeeperNetEx)
- [Curator (Java 高级封装)](https://curator.apache.org/)
- 本项目代码：[examples/ZooKeeperDemo](file:///c:/Users/ZJN/Desktop/jl/project/examples/ZooKeeperDemo/Program.cs)
