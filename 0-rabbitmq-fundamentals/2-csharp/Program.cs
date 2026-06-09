using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqFundamentals;

// One binary driven by APP/ROLE env vars: producer API or a consumer.
// Using a single image keeps the Docker build simple — compose sets APP and ROLE per service.
var url = Environment.GetEnvironmentVariable("RABBITMQ_URL")
          ?? "amqp://starci:Cuong123_A@rabbitmq:5672";
var app = Environment.GetEnvironmentVariable("APP") ?? "producer";

/// <summary>
/// Connects to RabbitMQ with up to 10 retries (2 s between attempts).
/// Needed because the app container usually starts before the broker is ready.
/// </summary>
/// <returns>An open <see cref="IConnection"/> to the broker.</returns>
async Task<IConnection> ConnectWithRetryAsync()
{
    // Build factory from the full AMQP URL so all options (vhost, TLS) are carried automatically.
    var factory = new ConnectionFactory { Uri = new Uri(url) };
    for (var attempt = 1; attempt <= 10; attempt++)
    {
        try
        {
            return await factory.CreateConnectionAsync();
        }
        catch
        {
            Console.WriteLine($"broker not ready (attempt {attempt}/10), retrying...");
            // Wait 2 s to give the broker time to become ready before the next attempt.
            await Task.Delay(2000);
        }
    }
    throw new Exception("Failed to connect to RabbitMQ after 10 attempts");
}

if (app == "producer")
{
    var connection = await ConnectWithRetryAsync();
    var channel = await connection.CreateChannelAsync();
    // assertExchange equivalent: durable so the topology survives a broker restart.
    await channel.ExchangeDeclareAsync(Topology.ExchangeDirect, ExchangeType.Direct, durable: true);
    await channel.ExchangeDeclareAsync(Topology.ExchangeFanout, ExchangeType.Fanout, durable: true);
    await channel.ExchangeDeclareAsync(Topology.ExchangeTopic, ExchangeType.Topic, durable: true);
    Console.WriteLine("[producer-api] connected to broker, three exchanges declared");

    var builder = WebApplication.CreateBuilder(args);
    builder.WebHost.UseUrls($"http://0.0.0.0:{Environment.GetEnvironmentVariable("PORT") ?? "3000"}");
    var web = builder.Build();

    /// <summary>
    /// Serialises <paramref name="payload"/> to JSON bytes and publishes it to
    /// <paramref name="exchange"/> with <paramref name="routingKey"/>.
    /// Returns an envelope the HTTP caller can use for correlation.
    /// </summary>
    /// <param name="exchange">Target exchange name (declared above).</param>
    /// <param name="routingKey">Routing hint; fanout ignores it (pass empty string).</param>
    /// <param name="payload">Request body to forward as JSON bytes.</param>
    /// <returns>Anonymous envelope: exchange / routingKey / payload / publishedAt.</returns>
    async Task<object> Publish(string exchange, string routingKey, JsonElement payload)
    {
        // Serialise to UTF-8 bytes so every language track shares the same wire format.
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload));
        // The producer talks to an EXCHANGE, never a queue.
        await channel.BasicPublishAsync(exchange, routingKey, body);
        return new
        {
            exchange,
            routingKey,
            payload,
            publishedAt = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
        };
    }

    web.MapGet("/health", () => Results.Json(new { status = "ok" }));
    web.MapPost("/publish/direct/{routingKey}", async (string routingKey, JsonElement payload) =>
        Results.Json(await Publish(Topology.ExchangeDirect, routingKey, payload)));
    // Fanout ignores the routing key, so publish with an empty key "".
    web.MapPost("/publish/fanout", async (JsonElement payload) =>
        Results.Json(await Publish(Topology.ExchangeFanout, "", payload)));
    web.MapPost("/publish/topic/{routingKey}", async (string routingKey, JsonElement payload) =>
        Results.Json(await Publish(Topology.ExchangeTopic, routingKey, payload)));

    Console.WriteLine("[producer-api] listening");
    await web.RunAsync();
}
else
{
    var role = Environment.GetEnvironmentVariable("ROLE") ?? "direct";
    var connection = await ConnectWithRetryAsync();
    var channel = await connection.CreateChannelAsync();

    /// <summary>
    /// Shared consume helper: sets prefetch=1 for fair dispatch, then starts an
    /// async consumer loop that logs and manually acks every received message.
    /// </summary>
    /// <param name="queue">Queue to consume from (already declared and bound).</param>
    /// <param name="label">Log prefix, e.g. "[DIRECT]" or "[FANOUT][a]".</param>
    /// <param name="withRoutingKey">When true, appends [rk=...] to the log line.</param>
    async Task Consume(string queue, string label, bool withRoutingKey)
    {
        // Manual ack: an un-acked message is redelivered if the consumer crashes.
        // prefetchCount=1 → fair dispatch: broker hands one message at a time to each free consumer.
        await channel.BasicQosAsync(0, 1, false);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            var content = Encoding.UTF8.GetString(ea.Body.ToArray());
            Console.WriteLine(withRoutingKey
                ? $"{label}[rk={ea.RoutingKey}] {content}"
                : $"{label} {content}");
            await channel.BasicAckAsync(ea.DeliveryTag, false);
        };
        await channel.BasicConsumeAsync(queue, autoAck: false, consumer);
    }

    switch (role)
    {
        case "direct":
            await channel.ExchangeDeclareAsync(Topology.ExchangeDirect, ExchangeType.Direct, durable: true);
            await channel.QueueDeclareAsync(Topology.QueueDirect, durable: true, exclusive: false, autoDelete: false);
            // Direct routing: deliver only when the routing key EXACTLY equals this binding key.
            await channel.QueueBindAsync(Topology.QueueDirect, Topology.ExchangeDirect, Topology.DirectRoutingKey);
            Console.WriteLine("[consumer-direct] bound and waiting for messages");
            await Consume(Topology.QueueDirect, "[DIRECT]", true);
            break;
        case "fanout-a":
        case "fanout-b":
            var queue = role == "fanout-b" ? Topology.QueueFanoutB : Topology.QueueFanoutA;
            var inst = role == "fanout-b" ? "b" : "a";
            await channel.ExchangeDeclareAsync(Topology.ExchangeFanout, ExchangeType.Fanout, durable: true);
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
            // Fanout ignores the routing key, so bind with an empty key "".
            await channel.QueueBindAsync(queue, Topology.ExchangeFanout, "");
            Console.WriteLine($"[consumer-fanout-{inst}] bound and waiting for messages");
            await Consume(queue, $"[FANOUT][{inst}]", false);
            break;
        case "topic":
            await channel.ExchangeDeclareAsync(Topology.ExchangeTopic, ExchangeType.Topic, durable: true);
            await channel.QueueDeclareAsync(Topology.QueueTopic, durable: true, exclusive: false, autoDelete: false);
            // Topic pattern "logs.#" matches any routing key under "logs.".
            await channel.QueueBindAsync(Topology.QueueTopic, Topology.ExchangeTopic, Topology.TopicPattern);
            Console.WriteLine("[consumer-topic] bound and waiting for messages");
            await Consume(Topology.QueueTopic, "[TOPIC]", true);
            break;
        default:
            throw new Exception($"Unknown ROLE: {role}");
    }

    // Keep the consumer process alive to receive messages.
    await Task.Delay(Timeout.Infinite);
}
