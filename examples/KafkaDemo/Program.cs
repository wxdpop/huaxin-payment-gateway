// ============================================================================
// Kafka Demo 主入口
// ============================================================================
// 演示场景:
//   1. 创建 Topic(多分区)
//   2. 生产者发送消息(Key 决定分区)
//   3. 消费者消费(自动分配分区)
//   4. Consumer Group(同 Group 内分区负载均衡,不同 Group 独立消费)
//   5. Offset 提交(手动提交 vs 自动提交)
//   6. 顺序消费(相同 Key 进入同一分区)
//
// 运行模式:
//   1. InMemory 模式(默认):  无需 Kafka 服务,内存模拟
//      dotnet run --project examples/KafkaDemo
//   2. Real 模式:             连接真实 Kafka 服务(需 Docker 启动)
//      dotnet run --project examples/KafkaDemo -- real
//      或显式指定地址: dotnet run --project examples/KafkaDemo -- real localhost:29092
//
// ★ Docker 启动 Kafka(多 listener 模式 - 关键):
//   Kafka 有两类地址: LISTENERS(监听) + ADVERTISED_LISTENERS(广播给客户端)
//   - INTERNAL listener: Docker 内部用,广播 kafka:9092(host 无法解析)
//   - EXTERNAL listener: host 机器用,广播 localhost:29092(可解析)
//   客户端必须连 EXTERNAL listener,否则 broker 广播的地址无法解析导致连接失败
//
//   docker run -d --name kafka \
//     -p 9092:9092 -p 29092:29092 \
//     -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
//     -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:29092 \
//     -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:9092,EXTERNAL://localhost:29092 \
//     -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
//     -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
//     confluentinc/cp-kafka
//
// 学习路径:
//   1. 先读 IKafkaClient.cs 理解接口抽象 + InMemoryKafka 实现
//   2. 读 RealKafka.cs 理解 Confluent.Kafka 真实客户端适配
//   3. 回到本文件看完整 Demo 演示
// ============================================================================

using KafkaDemo;

// ============================================================================
// 解析命令行参数,选择运行模式
// ============================================================================
var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "memory";
// 默认连 EXTERNAL listener(localhost:29092),不能用 127.0.0.1:9092
//   ★踩坑: 9092 是 INTERNAL listener,broker 广播地址是 kafka:9092(Docker 内部 hostname)
//           host 机器无法解析 kafka:9092,导致 ProduceAsync 失败
var bootstrapServers = args.Length > 1 ? args[1] : "localhost:29092";

bool useReal = mode is "real" or "r";
Console.WriteLine("============================================================");
Console.WriteLine($"  Kafka C# Demo  ({(useReal ? $"真实模式 -> {bootstrapServers}" : "InMemory 内存模拟模式")})");
Console.WriteLine("============================================================\n");

// 学习要点: Real 模式下用唯一 RunId 区分每次运行
//   - Kafka 是持久化的,真实模式下 Offset 持久化到 __consumer_offsets Topic
//   - 同一 GroupId 多次运行会从上次 Offset 继续,导致看不到完整演示
//   - 用 RunId 后缀让每次运行都是新 Consumer Group + 新 Topic,从 earliest 开始消费
//   ★关键: Topic 也加 RunId 后缀,避免历史消息干扰(否则会消费到上一次运行的残留消息)
var runId = useReal ? "-" + DateTime.UtcNow.ToString("HHmmss") : "";
Console.WriteLine($"  RunId: {runId,-8} (用于区分 Consumer Group 和 Topic)\n");

// Topic 名(Real 模式加 RunId 后缀,避免历史消息干扰)
//   InMemory: 用原名(InMemory 是进程内模拟,每次运行都是新的)
//   Real: 加 RunId 后缀(每次运行新 Topic,auto.create 自动创建)
string T(string name) => useReal ? $"{name}{runId}" : name;

// 工厂方法:统一构造 Producer / Consumer
InMemoryKafka? inMemoryCluster = useReal ? null : new InMemoryKafka();

IKafkaProducer CreateProducer() =>
    useReal ? new RealKafkaProducer(bootstrapServers)
            : inMemoryCluster!.CreateProducer();

IKafkaConsumer CreateConsumer(string group) =>
    useReal ? new RealKafkaConsumer(bootstrapServers, group + runId)
            : inMemoryCluster!.CreateConsumer(group);

void EnsureTopic(string topic, int partitions = 3)
{
    // 学习要点: Topic 创建策略
    //   InMemory: 需显式 CreateTopic
    //   Real Kafka: 默认开启 auto.create.topics.enable,Broker 自动创建
    //              (默认分区数 1,本 Demo 受 Broker 配置控制)
    if (!useReal)
        inMemoryCluster!.CreateTopic(topic, partitions);
}

// ============================================================================
// 场景 1: 创建 Topic + 生产消息
// ============================================================================
Console.WriteLine("【场景 1】创建 Topic + 生产消息");
Console.WriteLine("------------------------------------------------------------");

var orderTopic = T("order-events");
EnsureTopic(orderTopic, partitions: 3);
Console.WriteLine($"  Topic: {orderTopic}");

using var producer = CreateProducer();

// 同一 Key 的消息会进入同一分区(顺序保证)
var orders = new[]
{
    new { Id = "order_001", Customer = "Alice", Amount = 100 },
    new { Id = "order_002", Customer = "Bob",   Amount = 200 },
    new { Id = "order_003", Customer = "Alice", Amount = 150 },  // 同 Alice
    new { Id = "order_004", Customer = "Carol", Amount = 300 },
    new { Id = "order_005", Customer = "Bob",   Amount = 50 }    // 同 Bob
};

foreach (var o in orders)
{
    var json = System.Text.Json.JsonSerializer.Serialize(o);
    var result = await producer.ProduceAsync(orderTopic, key: o.Id, value: json);
    Console.WriteLine($"  [生产] key={o.Id}, partition={result.Partition}, offset={result.Offset}");
}

Console.WriteLine();

// ============================================================================
// 场景 2: 消费者消费
// ============================================================================
Console.WriteLine("【场景 2】消费者消费(payment-service group)");
Console.WriteLine("------------------------------------------------------------");

using var consumer = CreateConsumer("payment-service");
consumer.Subscribe(orderTopic);

// 拉取 5 条消息
Console.WriteLine("消费消息:");
for (int i = 0; i < 5; i++)
{
    var msg = consumer.Consume(TimeSpan.FromSeconds(useReal ? 5 : 1));
    if (msg == null)
    {
        Console.WriteLine($"  [消费] 第 {i + 1} 条无消息");
        break;
    }
    Console.WriteLine($"  [消费] partition={msg.Partition}, offset={msg.Offset}, key={msg.Key}");
    Console.WriteLine($"           value={msg.Value}");
    // 手动提交 Offset
    consumer.Commit(msg);
}
Console.WriteLine();

// ============================================================================
// 场景 3: Consumer Group(已消费完,再拉为空)
// ============================================================================
Console.WriteLine("【场景 3】已消费完,继续拉取应为空");
Console.WriteLine("------------------------------------------------------------");

// 学习要点: 已提交 Offset 的 Consumer Group 再次拉取会等待新消息
//   - InMemory: 直接返回 null(无新消息)
//   - Real Kafka: Consume 阻塞直到超时,无新消息返回 null
var emptyMsg = consumer.Consume(TimeSpan.FromSeconds(useReal ? 3 : 1));
Console.WriteLine($"再次拉取: {(emptyMsg == null ? "无消息(Offset 已提交)" : "有消息")}\n");

// ============================================================================
// 场景 4: 不同 Consumer Group 独立消费
// ============================================================================
Console.WriteLine("【场景 4】不同 Consumer Group 独立消费(广播模式)");
Console.WriteLine("------------------------------------------------------------");
Console.WriteLine("analytics-service group 从头消费(不共享 Offset):");

// 学习要点: 用新的 GroupId(analytics-service + runId)
//   - Real Kafka: 新 GroupId 无提交 Offset,从 AutoOffsetReset=Earliest 开始
//   - InMemory: 新 GroupId 也是从头消费
//   ★踩坑: 新 Consumer Group 第一次 Consume 会触发 Rebalance(JoinGroup+SyncGroup),
//           可能 2 秒内返回 null,不能因 null 立即 break,否则会消费 0 条
//           修复: 单次超时调长(5s) + 连续 2 次 null 才退出 + 拉够期望数量也退出
using var analyticsConsumer = CreateConsumer("analytics-service");
analyticsConsumer.Subscribe(orderTopic);

int count = 0;
int consecutiveNull = 0;
var analyticsDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(useReal ? 15 : 3);
while (DateTime.UtcNow < analyticsDeadline && count < 5)
{
    var msg = analyticsConsumer.Consume(TimeSpan.FromSeconds(useReal ? 5 : 1));
    if (msg == null)
    {
        consecutiveNull++;
        if (consecutiveNull >= 2) break;  // 连续 2 次 null,认为无更多消息
        continue;
    }
    consecutiveNull = 0;
    count++;
    Console.WriteLine($"  [分析] partition={msg.Partition}, offset={msg.Offset}, key={msg.Key}");
    analyticsConsumer.Commit(msg);
}
Console.WriteLine($"analytics-service 共消费 {count} 条(独立消费完整 Topic)\n");

// ============================================================================
// 场景 5: 顺序消费演示
// ============================================================================
Console.WriteLine("【场景 5】顺序消费演示(相同 Key 进入同分区)");
Console.WriteLine("------------------------------------------------------------");

var userTopic = T("user-events");
EnsureTopic(userTopic, partitions: 3);
Console.WriteLine($"  Topic: {userTopic}");
using var userProducer = CreateProducer();

// 用户事件(同 userId 的 events 应按顺序消费)
var events = new[]
{
    (userId: "user_A", evt: "登录"),
    (userId: "user_A", evt: "下单"),
    (userId: "user_A", evt: "支付"),
    (userId: "user_B", evt: "登录"),
    (userId: "user_A", evt: "退出"),
    (userId: "user_B", evt: "下单"),
    (userId: "user_B", evt: "支付")
};

Console.WriteLine("生产用户事件:");
foreach (var e in events)
{
    var result = await userProducer.ProduceAsync(userTopic, key: e.userId, value: e.evt);
    Console.WriteLine($"  {e.userId} - {e.evt} -> partition={result.Partition}");
}
Console.WriteLine();

// 消费时相同 userId 的事件一定按生产顺序到达
Console.WriteLine("消费用户事件:");
using var userConsumer = CreateConsumer("user-tracker");
userConsumer.Subscribe(userTopic);

var byUser = new Dictionary<string, List<string>>();
int userEventCount = 0;
int userConsecutiveNull = 0;
var userDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(useReal ? 15 : 3);
while (DateTime.UtcNow < userDeadline && userEventCount < events.Length)
{
    var msg = userConsumer.Consume(TimeSpan.FromSeconds(useReal ? 5 : 1));
    if (msg == null)
    {
        userConsecutiveNull++;
        if (userConsecutiveNull >= 2) break;
        continue;
    }
    userConsecutiveNull = 0;
    userEventCount++;
    if (!byUser.ContainsKey(msg.Key!)) byUser[msg.Key!] = new List<string>();
    byUser[msg.Key!].Add(msg.Value);
    userConsumer.Commit(msg);
}

Console.WriteLine("\n按用户分组(验证同用户事件顺序):");
foreach (var (user, evts) in byUser)
{
    Console.WriteLine($"  {user}: {string.Join(" -> ", evts)}");
}

Console.WriteLine("\n============================================================");
Console.WriteLine("  Demo 完成,学习要点回顾:");
Console.WriteLine("  1. Topic + Partition(分区决定并行度)");
Console.WriteLine("  2. Key 决定分区(同 Key 顺序保证)");
Console.WriteLine("  3. Consumer Group 内分区负载均衡");
Console.WriteLine("  4. 不同 Group 独立消费(广播模式)");
Console.WriteLine("  5. Offset 手动提交(精确控制消费进度)");
if (useReal)
{
    Console.WriteLine("\n  真实模式注意点:");
    Console.WriteLine("    - Topic 默认开启 auto.create,无需显式 CreateTopic");
    Console.WriteLine("    - Producer 关闭前必须 Flush 防止丢消息");
    Console.WriteLine("    - 用唯一 RunId 后缀避免 Consumer Group Offset 冲突");
    Console.WriteLine("    - 新 Consumer Group 从 Earliest 重放所有历史消息");
}
Console.WriteLine("============================================================");
