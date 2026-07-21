namespace PaymentGateway.Infrastructure.Cache;

// ============================================================================
// ICacheService —— Redis-Cluster 缓存服务统一抽象
// ============================================================================
// 学习要点: 抽象屏蔽底层 Redis 客户端差异
//   - 业务层只依赖 ICacheService,不直接使用 StackExchange.Redis
//   - 便于替换实现 (如改用 CSRedis 或 Mock 测试)
// ============================================================================

public interface ICacheService
{
    /// <summary>获取缓存值 (反序列化为 T)</summary>
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default);

    /// <summary>设置缓存 (绝对过期时间)</summary>
    Task SetAsync<T>(
        string key,
        T value,
        TimeSpan? expiry = null,
        CancellationToken ct = default);

    /// <summary>
    /// 缓存穿透防护版获取 —— GetOrAddAsync
    /// 学习要点: 解决"缓存穿透"的核心 API
    ///   - 缓存未命中时调用 factory 查 DB
    ///   - DB 也查不到则写"空标记"防穿透,下次直接命中空标记跳过 DB
    /// </summary>
    Task<T?> GetOrAddAsync<T>(
        string key,
        Func<CancellationToken, Task<T?>> factory,
        TimeSpan? expiry = null,
        CancellationToken ct = default);

    /// <summary>删除缓存</summary>
    Task RemoveAsync(string key, CancellationToken ct = default);

    /// <summary>键是否存在</summary>
    Task<bool> ExistsAsync(string key, CancellationToken ct = default);
}
