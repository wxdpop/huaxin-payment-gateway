using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Application.EventBus;
using PaymentGateway.Domain.Accounts;
using PaymentGateway.Domain.Shared;
using PaymentGateway.Infrastructure.EventBus;
using PaymentGateway.Infrastructure.Metrics;
using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Api.Consumers;

// ============================================================================
// CreditAccountConsumer —— 入账消费者 (Api 层宿主)
// ============================================================================
// ★ 学习要点: 消费者放在 Api 层而非 Application 层的原因
//   1. CreditAccountConsumer 继承 KafkaConsumerService<TEvent> (Infrastructure 层)
//      如果放在 Application 层,会导致 Application → Infrastructure 循环依赖
//   2. 消费者本质是"宿主服务"(BackgroundService),随应用启动
//      Api 层是应用启动入口,适合托管 BackgroundService
//   3. 消费者通过 IServiceProvider 创建 scope 获取 Application/Domain 层的仓储
//      不破坏依赖方向: Api → Application → Domain (消费者只是编排者)
//
// 【资金安全双重保障】
//   1. ZK 分布式锁 (以 merchant_id 为 key) — 资金变更前互斥,防并发更新
//   2. DB 乐观锁 (Account.Version) — UPDATE WHERE version=旧值,CAS 校验
//
// 【幂等设计 — 防重复入账】
//   - 消费者可能因 Kafka At-Least-Once 重复消费同一事件
//   - 用 biz_no (订单号) 作为唯一键:
//     - 查流水表是否已存在 biz_no=orderNo 的记录 → 已存在直接返回成功
//     - DB 表 account_transactions.biz_no 唯一约束兜底
// ============================================================================

public class CreditAccountConsumer : KafkaConsumerService<PaymentSucceededEvent>
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<CreditAccountConsumer> _logger;

    public CreditAccountConsumer(
        IServiceProvider serviceProvider,
        IOptions<EventBusOptions> options,
        ILogger<CreditAccountConsumer> logger)
        : base(options, logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override string Topic => PaymentEventTopics.PaymentSucceeded;
    protected override string ConsumerGroup => "payment-credit-account";

    /// <summary>
    /// 处理支付成功事件 (公开入口,供内存模式 InMemoryEventBus 订阅调用)
    /// 学习要点: Kafka 模式下由基类 ExecuteAsync 反序列化消息后调用 protected HandleAsync
    ///   内存模式(UseInMemory=true)下无 Kafka,通过 InMemoryEventBus.Subscribe 直接调用此方法,
    ///   保证两种事件总线实现下入账逻辑一致(同一份代码,两种触发方式)
    /// </summary>
    public Task<bool> HandlePaymentSucceededAsync(PaymentSucceededEvent @event, CancellationToken ct)
        => HandleAsync(@event, ct);

    protected override async Task<bool> HandleAsync(PaymentSucceededEvent @event, CancellationToken ct)
    {
        _logger.LogInformation("入账消费开始: orderNo={OrderNo}, merchantId={MerchantId}, amount={Amount}",
            @event.OrderNo, @event.MerchantId, @event.Amount);

        // ★ 学习要点: 消费者内创建 Scope
        //   - KafkaConsumerService 是 Singleton (BackgroundService),不能直接注入 Scoped 服务
        //   - 通过 IServiceScopeFactory 创建 scope,获取 Scoped 仓储
        //   - 每个 Kafka 消息创建独立 scope,保证业务隔离
        using var scope = _serviceProvider.CreateScope();
        var accountRepo = scope.ServiceProvider.GetRequiredService<IAccountRepository>();
        var distributedLock = scope.ServiceProvider.GetRequiredService<IDistributedLock>();
        var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
        var eventBus = scope.ServiceProvider.GetRequiredService<IEventBus>();

        // ★ Step 1: 幂等校验 — 查流水表是否已存在 biz_no=orderNo 的记录
        //   学习要点: 通过查流水判断是否已入账 (而非查账户余额)
        //   原因: 账户余额无"已入账订单列表",流水表才是真相来源 (Source of Truth)
        var existingTx = await accountRepo.GetTransactionByBizNoAsync(@event.OrderNo, ct);
        if (existingTx != null)
        {
            _logger.LogInformation("入账已处理过(幂等): orderNo={OrderNo}", @event.OrderNo);
            return true;  // 已处理,返回成功不重复入账
        }

        // ★ Step 2: 获取 ZK 分布式锁 (以 merchant_id 为 key)
        //   学习要点: 资金变更必须加锁,否则并发更新导致余额错误
        //   锁 key 选择: merchant_id (而非 orderId)
        //     - 同一商户的并发入账串行化 (避免余额覆盖)
        //     - 不同商户可并行 (性能优化)
        var lockKey = $"account:{@event.MerchantId}";
        await using var lockHandle = await distributedLock.TryAcquireAsync(
            lockKey, TimeSpan.FromSeconds(10), ct);

        if (lockHandle == null)
        {
            // ZK 锁获取失败 → 返回 false 触发重试
            //   学习要点: 资金变更场景宁可慢,不可错
            //   锁失败重试比直接跳过更安全 (避免漏入账)
            _logger.LogWarning("入账 ZK 锁获取失败,将重试: merchantId={MerchantId}", @event.MerchantId);
            return false;
        }

        // ★ Step 3: 查询账户 (锁内查询,确保最新数据)
        var account = await accountRepo.GetByMerchantIdAsync(@event.MerchantId, ct);
        if (account == null)
        {
            _logger.LogError("账户不存在: merchantId={MerchantId}", @event.MerchantId);
            throw new BusinessException($"商户账户不存在: {@event.MerchantId}");
        }

        // ★ Step 4: 调用领域方法变更余额 (内部 Version++)
        //   学习要点: 聚合根封装业务规则,外部不能直接 set Balance
        //   Credit 方法内部: 校验金额 + 余额增加 + Version++ + UpdatedAt 刷新
        account.Credit(Money.Yuan(@event.Amount));

        // ★ Step 5: 创建账户流水 (biz_no = orderNo,幂等键)
        //   学习要点: 流水不可变 (immutable),只增不改
        //   balance_after 记录变更后余额,用于对账审计
        var transaction = AccountTransaction.Create(
            accountId: account.Id,
            orderId: @event.OrderId,
            txType: TransactionType.Credit,
            amount: Money.Yuan(@event.Amount),
            balanceAfter: account.Balance,
            bizNo: @event.OrderNo,
            remark: $"支付入账: {@event.ChannelCode}");

        // ★ Step 6: 保存事务 (余额更新 + 流水写入,同一事务保证一致性)
        //   学习要点: 事务边界 — 账户余额与流水必须同事务
        //   否则可能出现"余额变了但流水没记录"或"流水记录了但余额没变"的脏数据
        accountRepo.Update(account);
        await accountRepo.AddTransactionAsync(transaction, ct);
        await unitOfWork.SaveChangesAsync(ct);

        // ★ Step 7: 发布 AccountCreditedEvent (触发商户通知)
        //   学习要点: 事务提交后再发事件,避免事务回滚但消息已发
        var accountCreditedEvent = new AccountCreditedEvent
        {
            AccountId = account.Id,
            MerchantId = @event.MerchantId,
            OrderId = @event.OrderId,
            OrderNo = @event.OrderNo,
            Amount = @event.Amount,
            BalanceAfter = account.Balance.Value,
            BizNo = @event.OrderNo
        };

        await eventBus.PublishAsync(
            PaymentEventTopics.AccountCredited,
            accountCreditedEvent,
            ct);

        _logger.LogInformation(
            "入账成功: orderNo={OrderNo}, merchantId={MerchantId}, amount={Amount}, balanceAfter={Balance}",
            @event.OrderNo, @event.MerchantId, @event.Amount, account.Balance.Value);

        // ★ M6-3: 入账成功指标(累计入账数)
        PaymentMetrics.AccountCreditTotal.Inc();

        return true;
    }
}
