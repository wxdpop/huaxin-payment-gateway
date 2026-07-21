using Microsoft.Extensions.DependencyInjection;
using PaymentGateway.Application.Accounts;
using PaymentGateway.Application.Orders.Commands.CreateOrder;
using PaymentGateway.Application.Orders.Queries;
using PaymentGateway.Application.Payments;

namespace PaymentGateway.Application;

/// <summary>
/// Application 层 DI 注册扩展
/// 学习要点:
///   1. 各层独立提供 DependencyInjection 扩展类,Api 层只调用 services.AddApplication()
///      屏蔽内部注册细节,符合"开闭原则"
///   2. 本工程已移除 MediatR,应用服务接口(IXxxService)在此显式注册
///      相比 MediatR 自动扫描,显式注册更直观、可控、易调试
///   3. 单例服务适合无状态服务(如 OrderQueryService),Scoped 适合需要请求上下文的服务
/// </summary>
public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        // ★ 应用服务注册(CQRS Command Service)
        // 生命周期 Scoped: 与 ISqlSugarClient 保持一致,每个 HTTP 请求一个实例
        // 这样可以安全地注入 ICurrentUser(请求上下文)、IUnitOfWork(请求内事务)
        services.AddScoped<ICreateOrderService, CreateOrderService>();

        // ★ M5: 发起支付 & 退款应用服务
        services.AddScoped<IPayOrderService, PayOrderService>();
        services.AddScoped<IRefundOrderService, RefundOrderService>();

        // M3: 回调处理应用服务 (Scoped, 每个回调请求独立实例)
        services.AddScoped<HandleCallbackService>();

        // 注册查询服务(CQRS Query Service,无状态查询)
        // Scoped 与请求生命周期一致,避免 Singleton 持有 Scoped 依赖引发 Captive Dependency
        services.AddScoped<OrderQueryService>();
        services.AddScoped<AccountQueryService>();

        return services;
    }
}
