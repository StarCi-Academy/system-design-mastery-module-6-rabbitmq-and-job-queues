using StackExchange.Redis;

namespace RateLimitApi;

public readonly record struct RateCheckResult(bool Allowed, long Remaining, long RetryAfterMs);

/// <summary>
/// Edge-layer token bucket. The whole check-then-decrement runs as ONE atomic
/// Lua script in Redis (EVALSHA), so concurrent requests cannot interleave.
/// </summary>
public sealed class RateLimiter
{
    private readonly IDatabase _redis;
    private readonly int _capacity;
    private readonly int _refillPerSec;
    private byte[] _luaSha = Array.Empty<byte>();
    private readonly string _luaScript;

    public RateLimiter(IConnectionMultiplexer mux, IConfiguration config)
    {
        _redis = mux.GetDatabase();
        _capacity = config.GetValue("Rate:Capacity", 5);
        _refillPerSec = config.GetValue("Rate:RefillPerSec", 5);
        _luaScript = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "token-bucket.lua"));
    }

    // Preload the script so later calls send only the cached SHA.
    public async Task InitAsync(IConnectionMultiplexer mux)
    {
        var endpoint = mux.GetEndPoints()[0];
        _luaSha = (byte[])(await mux.GetServer(endpoint).ScriptLoadAsync(_luaScript))!;
    }

    // One atomic round-trip: returns (allowed, remaining, retryAfterMs).
    public async Task<RateCheckResult> CheckRateAsync(string clientKey)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var key = $"rl:{clientKey}";

        var raw = (RedisValue[])(await _redis.ScriptEvaluateAsync(
            _luaSha,
            new RedisKey[] { key },
            new RedisValue[] { _capacity, _refillPerSec, now, 1 }))!;

        return new RateCheckResult((long)raw[0] == 1, (long)raw[1], (long)raw[2]);
    }
}
