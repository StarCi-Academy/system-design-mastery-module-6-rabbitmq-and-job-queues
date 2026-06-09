// Redis-backed job queue in C# using Hangfire with Redis storage.
// One app, two roles selected by APP: "api" (job-api) or "worker".
// The HTTP contract is identical across all 4 languages of this lesson.
using System.Text.Json;
using Hangfire;
using Hangfire.Common;
using Hangfire.Redis.StackExchange;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

var role = Environment.GetEnvironmentVariable("APP") ?? "api";
var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST") ?? "localhost";
var redisPort = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";
var redisConn = $"{redisHost}:{redisPort}";
var port = Environment.GetEnvironmentVariable("PORT") ?? "3000";
var failRate = double.Parse(Environment.GetEnvironmentVariable("FAIL_RATE") ?? "0.3");
var processDelayMs = int.Parse(Environment.GetEnvironmentVariable("PROCESS_DELAY_MS") ?? "500");
var concurrency = int.Parse(Environment.GetEnvironmentVariable("WORKER_CONCURRENCY") ?? "1");

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var muxer = ConnectionMultiplexer.Connect(redisConn);
builder.Services.AddSingleton<IConnectionMultiplexer>(muxer);

// Initialize the global JobStorage so the static Hangfire client API
// (BackgroundJob.Enqueue/Schedule and JobStorage.Current used by /stats)
// works in the api role too — AddHangfireServer (worker-only) is not enough.
var redisStorage = new RedisStorage(muxer, new RedisStorageOptions { Prefix = "hf:" });
JobStorage.Current = redisStorage;

builder.Services.AddHangfire(cfg => cfg.UseStorage(redisStorage));

// Only the worker role runs a Hangfire server that actually processes jobs.
if (role == "worker")
{
    builder.Services.AddHangfireServer(options =>
    {
        options.WorkerCount = concurrency;
        options.Queues = new[] { "email" };
    });
}

var app = builder.Build();
var redis = muxer.GetDatabase();
EmailProcessor.Configure(failRate, processDelayMs, redis);

// When AutomaticRetry exhausts its attempts the job transitions to FailedState;
// this filter intercepts that transition and pushes a dead-letter record to the
// Redis-backed DLQ list (dlq:email) that /dlq and /stats read.
GlobalJobFilters.Filters.Add(new DlqElectStateFilter(redis));

app.MapGet("/health", () => Results.Json(new { status = "ok" }));

app.MapGet("/stats", () =>
{
    var api = JobStorage.Current.GetMonitoringApi();
    var email = new
    {
        waiting = (int)api.EnqueuedCount("email"),
        active = (int)api.ProcessingCount(),
        completed = (int)api.SucceededListCount(),
        failed = (int)api.FailedCount(),
        delayed = (int)api.ScheduledCount(),
    };
    var dlqLen = (int)redis.ListLength("dlq:email");
    return Results.Json(new { email, dlq = new { waiting = dlqLen } });
});

app.MapGet("/dlq", () =>
{
    var items = redis.ListRange("dlq:email", 0, 100);
    var result = items.Select((raw, i) =>
    {
        var doc = JsonSerializer.Deserialize<JsonElement>(raw!);
        return new
        {
            id = i.ToString(),
            name = "dead",
            data = doc.GetProperty("data"),
            failedReason = doc.GetProperty("failedReason").GetString(),
        };
    });
    return Results.Json(result);
});

app.MapPost("/jobs", (EmailRequest req) =>
{
    if (string.IsNullOrEmpty(req.To))
    {
        return Results.Json(new { error = "missing 'to'" }, statusCode: 400);
    }
    var id = BackgroundJob.Enqueue<EmailProcessor>(p => p.Send(req.To!, req.Subject, req.ForceFail));
    return Results.Json(new { id, name = "send", queue = "email" });
});

app.MapPost("/jobs/priority", (PriorityRequest req) =>
{
    if (string.IsNullOrEmpty(req.To))
    {
        return Results.Json(new { error = "missing 'to' or 'priority'" }, statusCode: 400);
    }
    var client = new BackgroundJobClient();
    // With Hangfire, priority is expressed by routing to a named queue.
    var id = client.Create(() => new EmailProcessor().Send(req.To!, null, false), new EnqueuedState("email"));
    return Results.Json(new { id, name = "send", priority = req.Priority, queue = "email" });
});

app.MapPost("/jobs/delayed", (DelayedRequest req) =>
{
    if (string.IsNullOrEmpty(req.To))
    {
        return Results.Json(new { error = "missing 'to' or 'delayMs'" }, statusCode: 400);
    }
    var id = BackgroundJob.Schedule<EmailProcessor>(
        p => p.Send(req.To!, null, false),
        TimeSpan.FromMilliseconds(req.DelayMs));
    return Results.Json(new { id, name = "send", delayMs = req.DelayMs, queue = "email" });
});

Console.WriteLine(role == "worker"
    ? $"worker listening on queue email (concurrency={concurrency})"
    : $"job-api listening on port {port}");

app.Run();

// EmailProcessor is the job handler. Throwing makes Hangfire retry with backoff;
// the final failure pushes a record to the Redis-backed DLQ list.
public class EmailProcessor
{
    private static double _failRate;
    private static int _processDelayMs;
    private static IDatabase? _redis;

    public static void Configure(double failRate, int processDelayMs, IDatabase redis)
    {
        _failRate = failRate;
        _processDelayMs = processDelayMs;
        _redis = redis;
    }

    [AutomaticRetry(Attempts = 2, DelaysInSeconds = new[] { 1, 2 })]
    [Queue("email")]
    public void Send(string to, string? subject, bool forceFail)
    {
        Console.WriteLine($"Processing job for {to}");
        Thread.Sleep(_processDelayMs);
        if (forceFail || Random.Shared.NextDouble() < _failRate)
        {
            // On a real production app this exception would bubble through Hangfire's
            // retry machinery; the IElectStateFilter (registered below) moves the
            // final failure to the DLQ list.
            throw new InvalidOperationException($"Simulated SMTP failure for {to}");
        }
        Console.WriteLine($"Job completed: sent email to {to}");
    }
}

public record EmailRequest(string? To, string? Subject, bool ForceFail = false);
public record PriorityRequest(string? To, int Priority);
public record DelayedRequest(string? To, int DelayMs);

// Pushes a job to the Redis DLQ list once Hangfire elects it into FailedState
// (i.e. AutomaticRetry attempts are exhausted) instead of letting it vanish.
public class DlqElectStateFilter : IElectStateFilter
{
    private readonly IDatabase _redis;

    public DlqElectStateFilter(IDatabase redis) => _redis = redis;

    public void OnStateElection(ElectStateContext context)
    {
        if (context.CandidateState is not FailedState failed)
        {
            return;
        }

        // The first job argument is the recipient ("to") for the Send(...) method.
        var args = context.BackgroundJob.Job.Args;
        var to = args.Count > 0 ? args[0]?.ToString() : null;
        var record = JsonSerializer.Serialize(new
        {
            data = new { to },
            failedReason = failed.Exception?.Message ?? "job failed",
        });
        _redis.ListRightPush("dlq:email", record);
    }
}
