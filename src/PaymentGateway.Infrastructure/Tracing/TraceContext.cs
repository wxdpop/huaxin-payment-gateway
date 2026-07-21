using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace PaymentGateway.Infrastructure.Tracing;

/// <summary>
/// 业务链路上下文 —— 自定义 Span 辅助工具
/// 学习要点:
///   1. OpenTelemetry .NET 基于 System.Diagnostics.Activity 实现 Trace
///      - ActivitySource: 注册业务埋点源,需在 JaegerTracingExtensions 中 .AddSource("PaymentGateway")
///      - Activity: 一个 Span,通过 Activity.Current 自动关联父 Span(上下文流转)
///   2. 本封装屏蔽 ActivitySource 创建细节,业务代码一行调用即可生成 Span
///   3. Span 应当:
///        - 命名: 业务动词 + 名词(如 "order.create")便于在 Jaeger UI 中查找
///        - 标签: 业务关键字段(订单号/金额)用于检索
///        - 事件: 关键节点(如 "idempotent_check_passed")记录离散事件
///        - 异常: 通过 SetStatus(Error) + RecordException 记录失败
/// </summary>
public static class TraceContext
{
    /// <summary>
    /// 业务 ActivitySource 实例(Singleton 静态字段,避免重复创建)
    /// 学习要点: ActivitySource 与 JaegerTracingExtensions.AddSource("PaymentGateway") 名称对应
    ///   命名建议: 用产品/服务名,避免与其他库冲突
    /// </summary>
    public static readonly ActivitySource ActivitySource =
        new("PaymentGateway", "1.0.0");

    /// <summary>
    /// 开启一个业务 Span,自动关联到当前父 Span
    /// </summary>
    /// <param name="name">Span 名称(建议使用业务动词+名词,如 "order.create")</param>
    /// <param name="tags">初始标签(可选)</param>
    /// <returns>Activity 实例,使用 using 自动结束 Span</returns>
    /// <example>
    /// <code>
    /// using var span = TraceContext.StartSpan("payment.callback.handle");
    /// span?.SetTag("order_id", orderId);
    /// // ... 业务逻辑 ...
    /// </code>
    /// </example>
    public static Activity? StartSpan(string name, params (string Key, object? Value)[] tags)
    {
        var activity = ActivitySource.StartActivity(name, ActivityKind.Internal);
        if (activity is null) return null;  // 未启用追踪时返回 null

        // 自动写入初始标签
        foreach (var (key, value) in tags)
        {
            activity.SetTag(key, value);
        }

        return activity;
    }

    /// <summary>
    /// 记录业务事件(Event) —— Span 上的离散标记,便于在 Jaeger Timeline 中查看
    /// </summary>
    /// <example>
    /// <code>
    /// TraceContext.AddEvent("idempotent_check_passed", ("biz_no", bizNo));
    /// </code>
    /// </example>
    public static void AddEvent(string name, params (string Key, object? Value)[] tags)
    {
        var activity = Activity.Current;
        if (activity is null) return;

        // 学习要点: ActivityTagsCollection 支持 Key-Value 列表
        var tagList = new ActivityTagsCollection(tags.Select(t => new KeyValuePair<string, object?>(t.Key, t.Value)));
        activity.AddEvent(new ActivityEvent(name, tags: tagList));
    }

    /// <summary>
    /// 标记当前 Span 为失败 —— 配合 RecordException 记录异常堆栈
    /// </summary>
    public static void RecordError(Exception ex, string? message = null)
    {
        var activity = Activity.Current;
        if (activity is null) return;

        // 学习要点: SetStatus(Error) 在 Jaeger UI 中 Span 会显示为红色
        activity.SetStatus(ActivityStatusCode.Error, message ?? ex.Message);
        activity.RecordException(ex);  // 自动写入 exception.type / stacktrace 标签
    }
}
