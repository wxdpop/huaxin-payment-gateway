namespace PaymentGateway.Application.Orders.Commands.CreateOrder;

/// <summary>
/// 创建订单应用服务接口 —— CQRS 的 Command 入口
/// 学习要点:
///   1. 此接口替代了原本的 MediatR.IRequest&lt;T&gt; + IRequestHandler&lt;T,R&gt; 双对象模式
///      改为"接口 + 实现"单对象模式,职责更清晰
///   2. 接口定义在 Application 层,实现在同一层(CreateOrderService)
///      Api 层只依赖 ICreateOrderService(依赖倒置),不依赖具体实现
///   3. 单体架构中,模块间同步调用通过此类接口完成
///      未来拆分微服务时,只需将实现替换为 Http/Grpc 客户端即可,接口不变
///   4. 方法签名显式声明 CancellationToken,体现可取消异步的契约
/// </summary>
public interface ICreateOrderService
{
    /// <summary>
    /// 创建订单
    /// </summary>
    /// <param name="command">创建订单命令</param>
    /// <param name="ct">取消令牌</param>
    /// <returns>创建结果(订单号/状态等)</returns>
    /// <exception cref="PaymentGateway.Shared.Exceptions.BusinessException">
    /// 抛出业务异常时 Api 层返回 400(如订单号重复)
    /// </exception>
    Task<CreateOrderResult> CreateAsync(CreateOrderCommand command, CancellationToken ct = default);
}
