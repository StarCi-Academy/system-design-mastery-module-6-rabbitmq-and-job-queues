using StackExchange.Redis;

namespace RateLimitApi;

/// <summary>
/// Worker-layer limiter: a leaky-bucket pull loop drains the shared Redis list
/// at most LimitMax jobs per window then paces — protecting downstream.
/// </summary>
public sealed class ThrottledWorker : BackgroundService
{
    public const string QueueKey = "q:tasks";
    public const string CompletedKey = "q:tasks:completed";

    private readonly IDatabase _redis;
    private readonly int _max;
    private readonly int _windowMs;

    public ThrottledWorker(IConnectionMultiplexer mux, IConfiguration config)
    {
        _redis = mux.GetDatabase();
        _max = config.GetValue("Worker:LimitMax", 10);
        _windowMs = config.GetValue("Worker:LimitDurationMs", 1000);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine($"worker started: limiter {_max}/{_windowMs}ms");
        while (!stoppingToken.IsCancellationRequested)
        {
            var start = DateTimeOffset.UtcNow;
            for (var i = 0; i < _max; i++)
            {
                var job = await _redis.ListLeftPopAsync(QueueKey);
                if (job.IsNull)
                {
                    break;
                }
                await _redis.StringIncrementAsync(CompletedKey);
                Console.WriteLine($"job {job} completed at {DateTimeOffset.UtcNow:O}");
            }
            var elapsed = (DateTimeOffset.UtcNow - start).TotalMilliseconds;
            if (elapsed < _windowMs)
            {
                await Task.Delay(_windowMs - (int)elapsed, stoppingToken);
            }
        }
    }
}
