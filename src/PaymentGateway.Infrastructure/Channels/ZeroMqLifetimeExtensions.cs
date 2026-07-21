using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NetMQ;

namespace PaymentGateway.Infrastructure.Channels;

/// <summary>
/// ZeroMQ 生命周期管理 —— 通过 IHostedService 注册应用退出时的资源清理
/// 学习要点:
///   1. NetMQ 内部启动后台 Poller 线程,应用退出时需显式调用 NetMQConfig.Cleanup()
///      否则进程会卡死 5 秒后才被强杀(.NET 默认 Shutdown 超时)
///   2. 用 IHostedService (StopAsync) 实现清理比 IApplicationBuilder 扩展更通用:
///        - 不依赖 ASP.NET Core (Infrastructure 层可独立于 Web)
///        - IHostedService 在 Worker Service 等非 Web 场景同样适用
///   3. StopAsync 触发时机:
///        - SIGTERM 信号 (Docker stop / K8s pod termination)
///        - Ctrl+C 控制台关闭
///        - IHostApplicationLifetime.StopApplication() 调用
///      此回调在 Service Provider 仍可用时执行,可安全访问单例服务
/// </summary>
public class ZeroMqCleanupHostedService : IHostedService
{
    private readonly bool _enabled;

    public ZeroMqCleanupHostedService(IConfiguration configuration)
    {
        // 学习要点: 通过配置决定是否启用清理,与 ZeroMq:Enabled 联动
        _enabled = configuration.GetValue<bool>("ZeroMq:Enabled", true);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // 启动时无需操作,socket 由各自的 Client 单例负责初始化
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_enabled)
        {
            // ★ 学习要点: NetMQConfig.Cleanup 是全局静态方法 (无参数版)
            //   该方法会停止 NetMQ 内部的 Poller 线程并释放所有 socket 资源
            //   注意: NetMQ 4.x 的 Cleanup() 不接受 throwException 参数,
            //     异常会以 UnobservedTaskException 形式记录到日志,不影响进程退出
            try
            {
                NetMQConfig.Cleanup();
            }
            catch
            {
                // 忽略清理过程中可能的异常,确保进程能正常退出
            }
        }

        return Task.CompletedTask;
    }
}
