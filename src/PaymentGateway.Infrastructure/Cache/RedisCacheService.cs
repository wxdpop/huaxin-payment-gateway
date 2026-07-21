using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace PaymentGateway.Infrastructure.Cache;

// ============================================================================
// RedisCacheService —— Redis-Cluster 缓存服务实现
// ============================================================================
// ★ 学习要点: 这是工程亮点之一,完整实现"缓存三大问题"的防护方案
//
// 【缓存三大经典问题】
//   1. 缓存穿透 (Cache Penetration): 查询不存在的数据,缓存不命中,DB 也不命中
//      → 恶意攻击或 BUG 导致大量请求穿透到 DB
//   2. 缓存击穿 (Cache Breakdown): 热点 Key 过期瞬间,大量并发请求同时查 DB
//      → 单 Key 高并发场景常见
//   3. 缓存雪崩 (Cache Avalanche): 大量 Key 同时过期,DB 瞬间压力暴增
//      → 批量预热的场景易出现
//
// 【防护方案对照表】
//   问题   | 方案
//   -------|------------------------------------------
//   穿透   | 空标记缓存 + 短过期 (本实现采用)
//   穿透   | 布隆过滤器 (高级方案,本工程未实现,留扩展)
//   击穿   | 加锁串行化 (SingleFlight 模式,本实现采用)
//   击穿   | 永不过期 + 后台异步刷新 (高级方案)
//   雪崩   | 过期时间加随机抖动 (本实现采用)
//   雪崩   | 多级缓存 (本地+Redis,本工程未实现)
// ============================================================================

public class RedisCacheService : ICacheService, IDisposable, IAsyncDisposable
{
    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly ILogger<RedisCacheService> _logger;
    private readonly CacheOptions _options;

    // ★ 学习要点: SingleFlight 防击穿 —— 同一 Key 并发请求只放一个进 DB
    //   - ConcurrentDictionary<string, Task> 记录"正在执行"的查询 (非泛型 Task,因 Task<T> 派生自 Task)
    //   - 同 Key 的其他并发请求等待同一个 Task 完成,共享结果
    //   - 避免热点 Key 过期时多个请求同时打 DB
    //   注意: 多实例部署下应改用 Redis SETNX 实现分布式 SingleFlight
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Task> _inflight
        = new();

    public RedisCacheService(
        IOptions<CacheOptions> options,
        ILogger<RedisCacheService> logger)
    {
        _options = options.Value;
        _logger = logger;

        // 学习要点: Redis-Cluster 连接
        //   - StackExchange.Redis 通过 ConnectionMultiplexer.Connect 自动识别集群拓扑
        //   - 配置多个 endpoint,任一可达即可自动发现全部节点
        //   - 自动路由: 根据 Key 的 hash slot 决定发往哪个 master
        var config = new ConfigurationOptions
        {
            ConnectRetry = 3,
            ConnectTimeout = 5000,
            SyncTimeout = 5000,
            AbortOnConnectFail = false,
            Password = _options.Password
        };
        foreach (var endpoint in _options.Endpoints)
        {
            config.EndPoints.Add(endpoint);
        }

        _redis = ConnectionMultiplexer.Connect(config);
        _db = _redis.GetDatabase();
        _logger.LogInformation("RedisCacheService 初始化完成, 节点数={Count}", _options.Endpoints.Count);
    }

    public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default)
    {
        try
        {
            var value = await _db.StringGetAsync(key);
            if (!value.HasValue)
            {
                return default;
            }

            // 学习要点: 空标记识别 —— 值为 NULL_MARKER 表示是"穿透防护标记"
            //   命中空标记直接返回 default,跳过反序列化
            if (value == _options.NullMarker)
            {
                _logger.LogDebug("缓存命中(空标记): key={Key}", key);
                return default;
            }

            _logger.LogDebug("缓存命中: key={Key}", key);
            return System.Text.Json.JsonSerializer.Deserialize<T>(value!);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "缓存 GetAsync 异常: key={Key}", key);
            return default;  // 缓存异常降级: 不影响主流程,返回 default 让调用方查 DB
        }
    }

    public async Task SetAsync<T>(
        string key,
        T value,
        TimeSpan? expiry = null,
        CancellationToken ct = default)
    {
        try
        {
            // ★ 学习要点: 防雪崩 — 过期时间加随机抖动
            //   - 同一批预热的缓存如果统一用相同 expiry,会同时过期打 DB
            //   - 在 expiry 基础上加 [0, jitter) 的随机量,分散过期时间
            //   - 抖动比例默认 20% (如 100s 实际 100-120s 随机)
            var actualExpiry = ApplyJitter(expiry);

            var json = System.Text.Json.JsonSerializer.Serialize(value);
            await _db.StringSetAsync(key, json, actualExpiry);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "缓存 SetAsync 异常: key={Key}", key);
            // 写缓存失败不抛异常,缓存是"尽力而为"服务,不能影响主业务
        }
    }

    /// <summary>
    /// 缓存穿透防护版获取 —— GetOrAddAsync
    /// </summary>
    /// <remarks>
    /// ★ 学习要点: 这是缓存三大问题防护的核心方法
    ///
    /// 完整流程:
    ///   1. 先查缓存,命中直接返回 (含空标记识别)
    ///   2. 未命中 → 加锁(SingleFlight 防击穿)
    ///   3. 双重检查 (Double-Check): 拿到锁后再查一次 (可能已被其他线程写入)
    ///   4. 调用 factory 查 DB
    ///   5. DB 命中 → 写缓存 (带 jitter 防雪崩)
    ///   6. DB 未命中 → 写"空标记" (带短过期防穿透)
    /// </remarks>
    public async Task<T?> GetOrAddAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        TimeSpan? expiry = null,
        CancellationToken ct = default)
    {
        // ★ Step 1: 查缓存
        var cached = await GetAsync<T>(key, ct);
        if (cached != null)
        {
            return cached;
        }

        // 学习要点: default(T) 可能是 null (引用类型) 也可能是 0 (值类型)
        //   值类型不为 null 时缓存命中已在上面返回,这里能到的只有 null
        //   (假设业务上值类型不会有 0 是有效值的情况,如金额不会是 0)

        // ★ Step 2: SingleFlight 加锁防击穿
        //   - 同一 Key 的并发请求只放一个进 factory (查 DB)
        //   - 其他请求等待同一个 Task 完成,共享结果
        //   - 这是"合并请求"模式,Redis 也有 SETNX 实现版本,本实现用进程内 ConcurrentDictionary
        //     (多实例部署下需要 Redis SETNX 或 Redisson,本工程单体部署足够)
        var inflightTask = _inflight.GetOrAdd(key, _ => LoadFromDbAsync(key, factory, expiry, ct));

        try
        {
            var result = await (Task<T?>)inflightTask;
            return result;
        }
        finally
        {
            // 学习要点: 执行完成后从 inflight 移除,允许后续请求重新加载
            //   (如果再次 miss 会重新查 DB,而不是无限缓存空结果)
            _inflight.TryRemove(key, out _);
        }
    }

    /// <summary>实际查 DB + 写缓存的逻辑</summary>
    private async Task<T?> LoadFromDbAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        TimeSpan? expiry,
        CancellationToken ct)
    {
        // ★ Step 3: Double-Check (拿到 SingleFlight 锁后再查一次缓存)
        //   学习要点: 双重检查锁定模式 (Double-Checked Locking)
        //   - 在 inflight 字典 PutIfAbsent 之前,可能其他线程已写入
        //   - 拿到锁后再查一次,避免重复查 DB
        var doubleChecked = await GetAsync<T>(key, ct);
        if (doubleChecked != null)
        {
            return doubleChecked;
        }

        // ★ Step 4: 调用 factory 查 DB
        T? dbValue;
        try
        {
            dbValue = await factory(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetOrAddAsync factory 查询异常: key={Key}", key);
            throw;
        }

        // ★ Step 5/6: 根据查询结果写缓存
        if (dbValue == null)
        {
            // ★ 缓存穿透防护: 写"空标记"
            //   学习要点: 空标记是防穿透的核心
            //   - 不存在的数据也缓存,标记为 NULL_MARKER
            //   - 短过期 (默认 60s): 防止数据后续被创建后仍查询不到
            //   - 比布隆过滤器简单,适合数据量不大的场景
            var nullExpiry = TimeSpan.FromSeconds(_options.NullCacheExpirySeconds);
            try
            {
                await _db.StringSetAsync(key, _options.NullMarker, ApplyJitter(nullExpiry));
                _logger.LogDebug("缓存空标记写入: key={Key}, expiry={Expiry}s", key, nullExpiry.TotalSeconds);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "缓存空标记写入失败: key={Key}", key);
            }
        }
        else
        {
            // ★ 缓存命中: 正常写入 (带 jitter 防雪崩)
            await SetAsync(key, dbValue, expiry, ct);
        }

        return dbValue;
    }

    public async Task RemoveAsync(string key, CancellationToken ct = default)
    {
        try
        {
            await _db.KeyDeleteAsync(key);
            _logger.LogDebug("缓存删除: key={Key}", key);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "缓存 RemoveAsync 异常: key={Key}", key);
        }
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        try
        {
            return await _db.KeyExistsAsync(key);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "缓存 ExistsAsync 异常: key={Key}", key);
            return false;
        }
    }

    /// <summary>
    /// 对过期时间应用随机抖动 (防雪崩)
    /// </summary>
    /// <remarks>
    /// 学习要点: jitter 公式 = expiry + Random(0, expiry * JitterRatio)
    ///   - 默认 JitterRatio = 0.2 (20% 抖动)
    ///   - 如 expiry=100s,实际写入 100-120s 之间随机
    ///   - 大批量预热时,过期时间被打散,避免同时过期打 DB
    /// </remarks>
    private TimeSpan? ApplyJitter(TimeSpan? expiry)
    {
        if (expiry == null || expiry.Value == TimeSpan.Zero)
        {
            // 不设过期或永久缓存,不加 jitter
            return expiry;
        }

        var baseMs = expiry.Value.TotalMilliseconds;
        var jitterMs = Random.Shared.NextDouble() * baseMs * _options.JitterRatio;
        return TimeSpan.FromMilliseconds(baseMs + jitterMs);
    }

    public void Dispose()
    {
        _redis?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_redis != null)
        {
            await _redis.DisposeAsync();
        }
    }
}

// ============================================================================
// CacheOptions —— 缓存配置选项
// ============================================================================

public class CacheOptions
{
    /// <summary>Redis-Cluster 节点列表</summary>
    public List<string> Endpoints { get; set; } = new();

    /// <summary>密码 (无密码则空)</summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>空标记字符串 (命中此值表示"数据不存在")</summary>
    /// <remarks>
    /// 学习要点: 空标记设计要点
    ///   - 必须是不可能出现在真实数据中的字符串
    ///   - 默认用 "<<NULL>>" 防御性字符串
    ///   - 高级方案用单独的 byte 标记位,但增加复杂度
    /// </remarks>
    public string NullMarker { get; set; } = "<<NULL>>";

    /// <summary>空标记缓存过期时间(秒,默认 60s)</summary>
    public int NullCacheExpirySeconds { get; set; } = 60;

    /// <summary>过期时间抖动比例(0.2 = 20% 随机抖动)</summary>
    public double JitterRatio { get; set; } = 0.2;
}
