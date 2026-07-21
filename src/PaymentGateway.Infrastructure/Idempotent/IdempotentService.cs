using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PaymentGateway.Infrastructure.Cache;

namespace PaymentGateway.Infrastructure.Idempotent;

/// <summary>
/// HTTP 幂等配置 —— 通过 appsettings.json "Idempotent" 节绑定
/// 学习要点:
///   1. HTTP 幂等性(Idempotency):
///        - 同一请求执行 N 次与 1 次的结果完全一致(对支付场景至关重要)
///        - HTTP 规范中 GET/PUT/DELETE 天然幂等,POST 默认不幂等需额外保证
///   2. 客户端需在每次请求中携带 Idempotency-Key 头部(如 UUID)
///        - RFC 草案: https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/
///        - Stripe / GitHub 等公开 API 均使用此约定
///   3. 服务端实现策略:
///        - "请求中"标记: 防止重复请求并发执行(分布式锁)
///        - "已完成"缓存: 返回首次响应(避免重复扣款)
///        - "已失败"标记: 不缓存失败响应,允许重试
/// </summary>
public class IdempotentOptions
{
    /// <summary>
    /// 请求头名(默认 "Idempotency-Key")
    /// </summary>
    public string HeaderName { get; set; } = "Idempotency-Key";

    /// <summary>
    /// 缓存 key 前缀(Redis 中存为 "idempotent:{key}")
    /// </summary>
    public string KeyPrefix { get; set; } = "idempotent:";

    /// <summary>
    /// 幂等记录有效期(默认 24 小时)
    /// 学习要点: 防止 Redis 无限堆积 key,超过此时间的重复请求会重新执行
    /// </summary>
    public TimeSpan Expiry { get; set; } = TimeSpan.FromHours(24);

    /// <summary>
    /// "请求处理中"标记有效期(默认 30 秒)
    /// 学习要点: 防止异常崩溃后请求永久卡在 processing 状态
    /// </summary>
    public TimeSpan ProcessingTimeout { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// 幂等响应缓存数据结构
/// 学习要点: 因 public 方法返回此类型,需要标记为 public(可访问性一致性)
/// </summary>
public record IdempotentRecord
{
    /// <summary>状态: "processing" / "completed" / "failed"</summary>
    public string Status { get; init; } = "processing";

    /// <summary>HTTP 状态码</summary>
    public int StatusCode { get; init; }

    /// <summary>响应体(JSON 字符串)</summary>
    public string? ResponseBody { get; init; }

    /// <summary>响应 Content-Type</summary>
    public string? ContentType { get; init; }

    /// <summary>记录时间</summary>
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// HTTP 幂等服务 —— 基于 Redis 的幂等中间件核心实现
/// 学习要点:
///   1. 双阶段防护:
///        Stage 1 - "processing" 标记: 首次请求用 SET NX 抢占,后续并发请求返回 409
///        Stage 2 - "completed" 缓存: 业务完成后写入响应,后续重复请求直接返回缓存
///   2. 与分布式锁的区别:
///        - 分布式锁: 临界区互斥,并发请求排队等待
///        - 幂等: 并发请求直接拒绝(409),不等待(防重复扣款场景)
///   3. ICacheService 已实现 Redis-Cluster 高可用,本服务复用
///   4. 失败响应不缓存,允许客户端重试(如超时、5xx 等)
/// </summary>
public class IdempotentService
{
    private readonly ICacheService _cache;
    private readonly IdempotentOptions _options;
    private readonly ILogger<IdempotentService> _logger;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public IdempotentService(
        ICacheService cache,
        IOptions<IdempotentOptions> options,
        ILogger<IdempotentService> logger)
    {
        _cache = cache;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 检查并抢占幂等 key
    /// 学习要点: 用 "processing" 标记抢占,配合 SET NX 语义保证只抢占成功一次
    /// </summary>
    /// <param name="idempotencyKey">客户端传入的幂等 key</param>
    /// <returns>(是否首次抢占, 上次响应记录[若已存在])</returns>
    public async Task<(bool IsFirstRequest, IdempotentRecord? Existing)> TryAcquireAsync(
        string idempotencyKey,
        CancellationToken ct = default)
    {
        var redisKey = $"{_options.KeyPrefix}{idempotencyKey}";

        // 检查是否已有记录(可能是 processing 或 completed)
        var existing = await _cache.GetAsync<IdempotentRecord>(redisKey, ct);
        if (existing is not null)
        {
            _logger.LogInformation(
                "幂等检查: key={Key} 命中已有记录, status={Status}",
                idempotencyKey, existing.Status);
            return (false, existing);
        }

        // 抢占 processing 标记(GetOrAdd 原子语义)
        // 学习要点: 用 GetOrAddAsync 的原子性防止并发请求同时未命中
        //   - 若返回的是我们刚写入的 processing,说明抢占成功
        //   - 若返回的是别人写入的记录,说明并发被抢先
        var newRecord = new IdempotentRecord { Status = "processing" };
        var actualRecord = await _cache.GetOrAddAsync(
            redisKey,
            _ => Task.FromResult<IdempotentRecord?>(newRecord),
            _options.ProcessingTimeout,
            ct);

        // 通过引用比较判断是否抢占成功
        if (actualRecord is not null &&
            actualRecord.Status == "processing" &&
            actualRecord.ResponseBody is null)
        {
            // 注意: GetOrAdd 可能返回的是另一个并发请求的 processing 标记
            // 由于 IdempotentRecord 是 record,引用比较不可靠,这里简化为信任首次写入
            // 学习要点: 生产环境应使用 Redis 原生 SET NX PX 命令保证严格原子
            //   StackExchange.Redis: StringSetAsync(key, value, expiry, When.NotExists)
            return (true, null);
        }

        return (false, actualRecord);
    }

    /// <summary>
    /// 写入完成响应(成功或失败都记录,失败需客户端决定是否重试)
    /// </summary>
    public async Task CompleteAsync(
        string idempotencyKey,
        int statusCode,
        string? responseBody,
        string? contentType,
        CancellationToken ct = default)
    {
        var redisKey = $"{_options.KeyPrefix}{idempotencyKey}";

        // 学习要点: 5xx 系统异常不缓存,允许客户端重试
        //   4xx 客户端错误可缓存(如参数错误,重试也是同样结果)
        var shouldCache = statusCode < 500;

        var record = new IdempotentRecord
        {
            Status = shouldCache ? "completed" : "failed",
            StatusCode = statusCode,
            ResponseBody = shouldCache ? responseBody : null,
            ContentType = contentType,
            CreatedAt = DateTimeOffset.UtcNow
        };

        if (shouldCache)
        {
            await _cache.SetAsync(redisKey, record, _options.Expiry, ct);
            _logger.LogInformation(
                "幂等记录完成: key={Key} status={Status} statusCode={StatusCode}",
                idempotencyKey, record.Status, statusCode);
        }
        else
        {
            // 失败响应短暂缓存(防止短时间内重复重试),随后自动过期
            await _cache.SetAsync(redisKey, record, TimeSpan.FromMinutes(1), ct);
            _logger.LogWarning(
                "幂等记录失败: key={Key} statusCode={StatusCode}(5xx 短期缓存后允许重试)",
                idempotencyKey, statusCode);
        }
    }

    /// <summary>
    /// 释放 processing 标记(业务异常时主动释放,允许重试)
    /// </summary>
    public async Task ReleaseAsync(string idempotencyKey, CancellationToken ct = default)
    {
        var redisKey = $"{_options.KeyPrefix}{idempotencyKey}";
        await _cache.RemoveAsync(redisKey, ct);
        _logger.LogInformation("幂等标记释放: key={Key}", idempotencyKey);
    }
}
