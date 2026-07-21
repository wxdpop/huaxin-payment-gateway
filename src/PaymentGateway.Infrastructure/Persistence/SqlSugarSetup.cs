using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SqlSugar;

namespace PaymentGateway.Infrastructure.Persistence;

/// <summary>
/// SqlSugar 客户端配置扩展 —— 注册 ISqlSugarClient
/// 学习要点:
///   1. SqlSugarScope 是线程安全的单例,内部用 AsyncLocal 隔离每次请求的连接
///      (类似 HttpContext,看似单例,实则每次请求独立)
///   2. IsAutoCloseConnection=true: 自动管理连接(每次操作开启,完成关闭,连接池复用)
///   3. AOP 拦截器: SQL 执行日志/慢 SQL 监控等扩展点
///   4. DbType.MySql: 使用 MySqlConnector 驱动,SqlSugar 通过 MySqlConnector 间接依赖
///   5. 连接字符串关键参数:
///        Server=localhost         主机
///        Database=payment_gateway  数据库名
///        User ID=root             用户名
///        Password=123456          密码
///        Charset=utf8mb4          字符集(支持 emoji,中文场景必选)
///        Port=3306                 默认端口
///        SslMode=None             开发环境关闭 SSL
///        AllowPublicKeyRetrieval=True  MySQL 8.x 认证插件兼容
/// </summary>
public static class SqlSugarSetup
{
    public static IServiceCollection AddSqlSugar(this IServiceCollection services, IConfiguration configuration)
    {
        // 注册 ISqlSugarClient 为 Singleton
        // 学习要点: SqlSugarScope 单例 + AsyncLocal 隔离,是官方推荐做法
        //   1. 不必每次请求创建新实例(性能优)
        //   2. 跨请求隔离(线程安全)
        //   3. 也可注册为 Scoped,但单例性能更好
        services.AddSingleton<ISqlSugarClient>(sp =>
        {
            var connectionString = configuration.GetConnectionString("MySql")
                ?? throw new InvalidOperationException("未配置 MySql 连接字符串");

            var db = new SqlSugarScope(new ConnectionConfig
            {
                ConnectionString = connectionString,
                DbType = DbType.MySql,
                IsAutoCloseConnection = true,  // 自动开关连接(连接池复用)
                MoreSettings = new ConnMoreSettings
                {
                    IsAutoRemoveDataCache = true  // 自动清理数据缓存,避免脏读
                }
            },
            client =>
            {
                // 命令超时(秒),生产推荐 30
                client.Ado.CommandTimeOut = 30;

                // ★ AOP: SQL 执行日志(学习工程开启,生产环境按需关闭或改为 Debug 级别)
                //   可在此拦截慢 SQL,记录到日志/上报 Jaeger
                client.Aop.OnLogExecuting = (sql, pars) =>
                {
                    // 学习要点: 实际项目应注入 ILogger,这里简化用 Console
                    // M4 阶段会替换为 ILogger + 关联 TraceId
                    if (sql.Length > 200)
                        Console.WriteLine($"[SqlSugar SQL] {sql[..200]}...");
                    else
                        Console.WriteLine($"[SqlSugar SQL] {sql}");
                };

                // ★ AOP: 慢 SQL 监控(执行时间 > 1s 记录警告)
                client.Aop.OnLogExecuted = (sql, pars) =>
                {
                    var elapsed = client.Ado.SqlExecutionTime;
                    if (elapsed.TotalMilliseconds > 1000)
                    {
                        Console.WriteLine($"[SqlSugar 慢SQL] 耗时 {elapsed.TotalMilliseconds}ms, SQL: {sql[..Math.Min(100, sql.Length)]}");
                    }
                };
            });

            return db;
        });

        return services;
    }
}
