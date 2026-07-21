using Prometheus;

namespace PaymentGateway.Infrastructure.Metrics;

/// <summary>
/// 支付网关业务指标定义 —— Prometheus 自定义指标中心
/// 学习要点:
///   1. prometheus-net 采用"静态注册表"模型: Prometheus.Metrics.CreateCounter/Histogram 返回单例,
///      全局只创建一次(重复调用同 name 会返回已存在实例),业务代码直接 .Inc()/.Observe()
///   2. 指标命名规范(OpenMetrics 标准):
///        - 前缀: paymentgateway_ (应用名小写下划线)
///        - 单位后缀: 耗时类用 _seconds / _bytes,避免 Grafana 二次换算
///        - 总量类用 _total (Counter 自动追加,不要手动加)
///   3. 标签(label)设计: 高基数标签(如 order_id)严禁,否则时间序列爆炸
///      只用低基数标签(如 channel/status/type,值域 < 20)
///   4. 四种指标类型:
///        Counter: 单调递增(订单数/支付数),只增不减,reset=0 on 进程重启
///        Gauge: 可增可减(当前连接数/队列长度),适合瞬时值
///        Histogram: 分布统计(延迟),按 bucket 分桶,配合 histogram_quantile 算 P95
///        Summary: 客户端算分位数(不常用,开销大)
///   5. 指标暴露: Program.cs 中 app.UseHttpMetrics() + app.MapMetrics("/metrics")
///      Prometheus 定时 pull /metrics 端点采集
/// ★ 注意: 命名空间名 PaymentGateway.Infrastructure.Metrics 与 Prometheus.Metrics 类同名,
///   直接写 Metrics.CreateCounter 会被解析为命名空间而非类,故此处用全限定 Prometheus.Metrics
/// </summary>
public static class PaymentMetrics
{
    /// <summary>累计创建订单数(单调递增)</summary>
    public static readonly Counter OrdersCreatedTotal = Prometheus.Metrics
        .CreateCounter(
            "paymentgateway_orders_created_total",
            "累计创建订单总数");

    /// <summary>累计发起支付数(按渠道 label 区分: wechat/alipay/unionpay)</summary>
    public static readonly Counter PaymentsTotal = Prometheus.Metrics
        .CreateCounter(
            "paymentgateway_payments_total",
            "累计发起支付数",
            "channel");

    /// <summary>累计渠道回调处理数(按状态 label: success/alreadyhandled/processing)</summary>
    public static readonly Counter CallbacksTotal = Prometheus.Metrics
        .CreateCounter(
            "paymentgateway_callbacks_total",
            "累计渠道回调处理数",
            "status");

    /// <summary>分布式锁获取次数(按类型 type: redlock/zookeeper/dual,结果 result: success/fail)</summary>
    public static readonly Counter LockAcquireTotal = Prometheus.Metrics
        .CreateCounter(
            "paymentgateway_lock_acquire_total",
            "分布式锁获取次数",
            "type",
            "result");

    /// <summary>分布式锁获取耗时分布(秒,按类型 type 分桶)</summary>
    /// <remarks>Histogram 默认 bucket 覆盖 0.005~10s,适合锁场景</remarks>
    public static readonly Histogram LockAcquireDurationSeconds = Prometheus.Metrics
        .CreateHistogram(
            "paymentgateway_lock_acquire_duration_seconds",
            "分布式锁获取耗时(秒)",
            "type");

    /// <summary>累计账户入账数(支付成功后给商户账户加款)</summary>
    public static readonly Counter AccountCreditTotal = Prometheus.Metrics
        .CreateCounter(
            "paymentgateway_account_credit_total",
            "累计账户入账总数");
}
