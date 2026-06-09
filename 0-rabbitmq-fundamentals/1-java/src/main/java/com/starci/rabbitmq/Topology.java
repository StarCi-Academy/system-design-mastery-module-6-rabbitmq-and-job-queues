package com.starci.rabbitmq;

/**
 * Shared AMQP topology constants for the producer and every consumer.
 * Keeping names in one place ensures all processes declare the same exchanges,
 * queues, and binding keys — a mismatch would silently drop messages.
 */
public final class Topology {

    /** Utility class — not instantiable. */
    private Topology() {}

    /** Direct exchange: routes by an exact routing-key match (1:1 routing). */
    public static final String EXCHANGE_DIRECT = "orders.direct";
    /** Fanout exchange: ignores the routing key and broadcasts to every bound queue. */
    public static final String EXCHANGE_FANOUT = "events.fanout";
    /** Topic exchange: routes by wildcard pattern (* = one token, # = zero or more). */
    public static final String EXCHANGE_TOPIC = "logs.topic";

    /** Durable queue bound to the direct exchange for the direct consumer. */
    public static final String QUEUE_DIRECT = "orders.created.q";
    /** Durable queue for fanout consumer instance A — each instance needs its own queue. */
    public static final String QUEUE_FANOUT_A = "events.fanout.q.a";
    /** Durable queue for fanout consumer instance B. */
    public static final String QUEUE_FANOUT_B = "events.fanout.q.b";
    /** Durable queue bound to the topic exchange with TOPIC_PATTERN. */
    public static final String QUEUE_TOPIC = "logs.topic.q";

    /** Exact binding key for the direct queue; any other key is dropped by the broker. */
    public static final String DIRECT_ROUTING_KEY = "orders.created";
    /** Topic pattern: # matches zero or more dot-separated tokens. */
    public static final String TOPIC_PATTERN = "logs.#";
}
