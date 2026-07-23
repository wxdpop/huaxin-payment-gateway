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
// 运行:
//   dotnet run --project examples/KafkaDemo
//
// 真实环境替换:
//   Docker:
//     docker run -d --name kafka -p 9092:9092 \
//       -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
//       -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
//       confluentinc/cp-kafka
//   NuGet:
//     dotnet add package Confluent.Kafka
//   代码:
//     var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
//     using var producer = new ProducerBuilder<string, string>(config).Build();
//     await producer.ProduceAsync("order-events", new Message<string, string> { ... });
// ============================================================================

using KafkaDemo;

Console.WriteLine("============================================================");
Console.WriteLine("  Kafka C# Demo");
Console.WriteLine("  (使用 InMemoryKafka 模拟,无需启动真实 Kafka 服务)");
Console.WriteLine("============================================================\n");

// 创建 Kafka 实例(单进程模拟)
var kafka = new InMemoryKafka();

// ============================================================================
// 场景 1: 创建 Topic + 生产消息
// ============================================================================
Console.WriteLine("【场景 1】创建 Topic + 生产消息");
Console.WriteLine("------------------------------------------------------------");

kafka.CreateTopic("order-events", partitions: 3);

using var producer = kafka.CreateProducer();

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
    var result = await producer.ProduceAsync("order-events", key: o.Id, value: json);
    Console.WriteLine($"  [生产] key={o.Id}, partition={result.Partition}, offset={result.Offset}");
}

Console.WriteLine();

// ============================================================================
// 场景 2: 消费者消费
// ============================================================================
Console.WriteLine("【场景 2】消费者消费(payment-service group)");
Console.WriteLine("------------------------------------------------------------");

using var consumer = kafka.CreateConsumer(group: "payment-service");
consumer.Subscribe("order-events");

// 拉取 5 条消息
Console.WriteLine("消费消息:");
for (int i = 0; i < 5; i++)
{
    var msg = consumer.Consume(TimeSpan.FromSeconds(1));
    if (msg == null) break;
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

var emptyMsg = consumer.Consume(TimeSpan.FromSeconds(1));
Console.WriteLine($"再次拉取: {(emptyMsg == null ? "无消息(Offset 已提交)" : "有消息")}\n");

// ============================================================================
// 场景 4: 不同 Consumer Group 独立消费
// ============================================================================
Console.WriteLine("【场景 4】不同 Consumer Group 独立消费(广播模式)");
Console.WriteLine("------------------------------------------------------------");
Console.WriteLine("analytics-service group 从头消费(不共享 Offset):");

using var analyticsConsumer = kafka.CreateConsumer(group: "analytics-service");
analyticsConsumer.Subscribe("order-events");

int count = 0;
while (true)
{
    var msg = analyticsConsumer.Consume(TimeSpan.FromSeconds(1));
    if (msg == null) break;
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

kafka.CreateTopic("user-events", partitions: 3);
using var userProducer = kafka.CreateProducer();

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
    var result = await userProducer.ProduceAsync("user-events", key: e.userId, value: e.evt);
    Console.WriteLine($"  {e.userId} - {e.evt} → partition={result.Partition}");
}
Console.WriteLine();

// 消费时相同 userId 的事件一定按生产顺序到达
Console.WriteLine("消费用户事件:");
using var userConsumer = kafka.CreateConsumer(group: "user-tracker");
userConsumer.Subscribe("user-events");

var byUser = new Dictionary<string, List<string>>();
while (true)
{
    var msg = userConsumer.Consume(TimeSpan.FromSeconds(1));
    if (msg == null) break;
    if (!byUser.ContainsKey(msg.Key!)) byUser[msg.Key!] = new List<string>();
    byUser[msg.Key!].Add(msg.Value);
    userConsumer.Commit(msg);
}

Console.WriteLine("\n按用户分组(验证同用户事件顺序):");
foreach (var (user, evts) in byUser)
{
    Console.WriteLine($"  {user}: {string.Join(" → ", evts)}");
}

Console.WriteLine("\n============================================================");
Console.WriteLine("  Demo 完成,学习要点回顾:");
Console.WriteLine("  1. Topic + Partition(分区决定并行度)");
Console.WriteLine("  2. Key 决定分区(同 Key 顺序保证)");
Console.WriteLine("  3. Consumer Group 内分区负载均衡");
Console.WriteLine("  4. 不同 Group 独立消费(广播模式)");
Console.WriteLine("  5. Offset 手动提交(精确控制消费进度)");
Console.WriteLine("============================================================");
