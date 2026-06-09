namespace RabbitMqFundamentals;

/// <summary>
/// Shared AMQP topology constants for the producer and every consumer.
/// Keeping names in one place ensures all processes declare the same exchanges,
/// queues, and binding keys — a mismatch would silently drop messages.
/// </summary>
public static class Topology
{
    /// <summary>Direct exchange: routes by an exact routing-key match (1:1 routing).</summary>
    public const string ExchangeDirect = "orders.direct";

    /// <summary>Fanout exchange: ignores the routing key and broadcasts to every bound queue.</summary>
    public const string ExchangeFanout = "events.fanout";

    /// <summary>Topic exchange: routes by wildcard pattern (* = one token, # = zero or more).</summary>
    public const string ExchangeTopic = "logs.topic";

    /// <summary>Durable queue bound to the direct exchange for the direct consumer.</summary>
    public const string QueueDirect = "orders.created.q";

    /// <summary>Durable queue for fanout consumer instance A — each instance needs its own queue.</summary>
    public const string QueueFanoutA = "events.fanout.q.a";

    /// <summary>Durable queue for fanout consumer instance B.</summary>
    public const string QueueFanoutB = "events.fanout.q.b";

    /// <summary>Durable queue bound to the topic exchange with <see cref="TopicPattern"/>.</summary>
    public const string QueueTopic = "logs.topic.q";

    /// <summary>Exact binding key for the direct queue; any other key is dropped by the broker.</summary>
    public const string DirectRoutingKey = "orders.created";

    /// <summary>Topic pattern: # matches zero or more dot-separated tokens.</summary>
    public const string TopicPattern = "logs.#";
}
