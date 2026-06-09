using RateLimitApi;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Allow env overrides: REDIS_HOST/REDIS_PORT, APP, RATE_*, LIMIT_*.
var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST")
    ?? builder.Configuration.GetValue("Redis:Host", "localhost");
var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT")
    ?? builder.Configuration.GetValue("Redis:Port", 6379).ToString();
var role = Environment.GetEnvironmentVariable("APP") ?? "api";

// The multiplexer is thread-safe and expensive: register one singleton.
var mux = await ConnectionMultiplexer.ConnectAsync($"{redisHost}:{redisPort}");
builder.Services.AddSingleton<IConnectionMultiplexer>(mux);

if (role == "worker")
{
    builder.Services.AddHostedService<ThrottledWorker>();
    var workerApp = builder.Build();
    await workerApp.RunAsync();
    return;
}

builder.Services.AddSingleton<RateLimiter>();
var app = builder.Build();

var limiter = app.Services.GetRequiredService<RateLimiter>();
await limiter.InitAsync(mux);
var db = mux.GetDatabase();

app.MapGet("/health", () => Results.Json(new { status = "ok" }));

// Per-client token-bucket check; 200 when allowed, 429 when limited.
app.MapGet("/api/data", async (HttpContext ctx) =>
{
    var clientKey = ctx.Request.Headers["X-Client-Id"].FirstOrDefault();
    if (string.IsNullOrEmpty(clientKey))
    {
        clientKey = "anon";
    }
    var r = await limiter.CheckRateAsync(clientKey);
    if (!r.Allowed)
    {
        return Results.Json(
            new { message = "Too Many Requests", retryAfterMs = r.RetryAfterMs, remaining = r.Remaining },
            statusCode: StatusCodes.Status429TooManyRequests);
    }
    return Results.Json(new
    {
        data = new { value = Random.Shared.NextDouble(), at = DateTimeOffset.UtcNow.ToString("O") },
        rate = new { remaining = r.Remaining },
    });
});

// Enqueue a batch of jobs onto the shared Redis list queue.
app.MapPost("/jobs/batch", async (BatchRequest body) =>
{
    for (var i = 0; i < body.Count; i++)
    {
        await db.ListRightPushAsync(ThrottledWorker.QueueKey, i.ToString());
    }
    return Results.Json(new { enqueued = body.Count });
});

app.MapGet("/stats", async () =>
{
    var waiting = await db.ListLengthAsync(ThrottledWorker.QueueKey);
    var completedRaw = await db.StringGetAsync(ThrottledWorker.CompletedKey);
    var completed = completedRaw.IsNull ? 0 : (long)completedRaw;
    return Results.Json(new { waiting, active = 0, completed });
});

var port = Environment.GetEnvironmentVariable("PORT") ?? "3000";
app.Run($"http://0.0.0.0:{port}");

internal sealed record BatchRequest(int Count);
