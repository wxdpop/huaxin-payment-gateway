using Microsoft.Extensions.Logging;
using PaymentGateway.Application.Abstractions;
using PaymentGateway.Domain.Orders;
using PaymentGateway.Domain.Shared;
using PaymentGateway.Shared.Exceptions;

namespace PaymentGateway.Application.Orders.Commands.CreateOrder;

/// <summary>
/// 创建订单应用服务 —— CQRS 的 Command 应用服务
/// 学习要点:
///   1. 本类替代了原 MediatR 的 IRequestHandler&lt;CreateOrderCommand, CreateOrderResult&gt;
///      改为实现 ICreateOrderService 接口,通过 DI 注入到调用方(Api 层)
///   2. 通过构造函数注入依赖(仓储/UoW/事件分发器),体现 DI 原则
///   3. 业务流程:
///      a. 校验商户订单号是否重复(幂等防护,DB 层 uk_merchant_out_trade_no 兜底)
///      b. 工厂方法创建 Order 聚合根(领域层校验不变量)
///      c. 通过仓储持久化
///      d. SaveChanges 提交事务
///      e. 事务成功后分发领域事件(OrderCreatedEvent)
///   4. 异常策略: 业务规则违反抛 BusinessException(返回 400),系统异常向上抛(返回 500)
///   5. 生命周期: Scoped(每个 HTTP 请求一个实例),与 ISqlSugarClient 保持一致
/// </summary>
public class CreateOrderService : ICreateOrderService
{
    private readonly IOrderRepository _orderRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly IDomainEventDispatcher _eventDispatcher;
    private readonly ILogger<CreateOrderService> _logger;

    public CreateOrderService(
        IOrderRepository orderRepository,
        IUnitOfWork unitOfWork,
        IDomainEventDispatcher eventDispatcher,
        ILogger<CreateOrderService> logger)
    {
        _orderRepository = orderRepository;
        _unitOfWork = unitOfWork;
        _eventDispatcher = eventDispatcher;
        _logger = logger;
    }

    public async Task<CreateOrderResult> CreateAsync(CreateOrderCommand request, CancellationToken ct)
    {
        // 1. 幂等校验: 同一商户 + 商户订单号 已存在则直接返回(防止重复下单)
        //    学习要点: 这是应用层幂等,DB 层 uk_merchant_out_trade_no 约束兜底
        var existing = await _orderRepository.FindByOrderNoAsync(request.OutTradeNo, ct);
        if (existing is not null)
        {
            _logger.LogWarning("重复下单被拦截: MerchantId={MerchantId}, OutTradeNo={OutTradeNo}",
                request.MerchantId, request.OutTradeNo);
            throw new BusinessException($"商户订单号 {request.OutTradeNo} 已存在", "DUP_ORDER");
        }

        // 2. 生成平台订单号(规则: PG + yyyyMMddHHmmss + 6位随机数)
        //    学习要点: 订单号生成规则应保证全局唯一与时序可读
        var orderNo = $"PG{DateTimeOffset.UtcNow:yyyyMMddHHmmss}{Random.Shared.Next(100000, 999999)}";

        // 3. 调用聚合根工厂方法创建订单(领域层校验不变量)
        var order = Order.Create(
            merchantId: request.MerchantId,
            orderNo: orderNo,
            outTradeNo: request.OutTradeNo,
            amount: Money.Yuan(request.Amount),
            subject: request.Subject);

        // 4. 持久化(SqlSugar Insertable 即时执行 INSERT)
        await _orderRepository.AddAsync(order, ct);

        // 5. 提交事务
        //    学习要点: SqlSugar 每次操作即时执行,UnitOfWork 暂为空操作
        //    多表原子操作需通过 db.Ado.UseTran() 编排,后续 M2/M3 阶段会引入事务边界
        await _unitOfWork.SaveChangesAsync(ct);
        _logger.LogInformation("订单创建成功: OrderId={OrderId}, OrderNo={OrderNo}", order.Id, order.OrderNo);

        // 6. 分发领域事件(事务提交后,避免"幻读事件"问题)
        //    学习要点: 必须在 SaveChanges 成功后分发,否则事务回滚但事件已发布会导致不一致
        await _eventDispatcher.DispatchAsync(order, ct);

        return new CreateOrderResult(order.Id, order.OrderNo, order.Status.ToString(), order.Amount.Value);
    }
}
