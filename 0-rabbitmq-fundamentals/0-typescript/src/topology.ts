/**
 * Shared AMQP topology constants — kept in one place so the producer and every
 * consumer declare the exact same exchanges, queues, and binding keys.
 * Changing a name here automatically propagates to all services.
 */

/** Names of the three AMQP exchanges used in this lesson. */
export const EXCHANGES = {
  /** Direct exchange: routes by an exact routing-key match. */
  DIRECT: "orders.direct",
  /** Fanout exchange: ignores the routing key and broadcasts to every bound queue. */
  FANOUT: "events.fanout",
  /** Topic exchange: routes by wildcard pattern (* = one token, # = zero-or-more). */
  TOPIC: "logs.topic",
} as const

/**
 * AMQP exchange type strings.
 * Must match the values expected by amqplib's assertExchange call.
 */
export enum ExchangeKind {
  /** Routes messages to queues whose binding key exactly matches the routing key. */
  Direct = "direct",
  /** Broadcasts every message to all bound queues regardless of routing key. */
  Fanout = "fanout",
  /** Routes by pattern: * matches one token, # matches zero or more tokens. */
  Topic = "topic",
}

/** Names of the four durable queues — one per consumer role. */
export const QUEUES = {
  /** Receives messages from orders.direct with key orders.created. */
  DIRECT: "orders.created.q",
  /** Instance A of the fanout consumer — gets every fanout message. */
  FANOUT_A: "events.fanout.q.a",
  /** Instance B of the fanout consumer — gets every fanout message. */
  FANOUT_B: "events.fanout.q.b",
  /** Receives messages from logs.topic matching the logs.# pattern. */
  TOPIC: "logs.topic.q",
} as const

/** Exact routing key that binds the direct queue. Anything else is dropped. */
export const DIRECT_ROUTING_KEY = "orders.created"
/** Topic pattern: # matches zero or more dot-separated tokens. */
export const TOPIC_PATTERN = "logs.#"
