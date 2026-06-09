import * as amqp from "amqplib"
import {
  EXCHANGES,
  ExchangeKind,
  QUEUES,
  DIRECT_ROUTING_KEY,
  TOPIC_PATTERN,
} from "./topology"

/**
 * One consumer binary driven by the ROLE env var: direct | fanout-a | fanout-b | topic.
 * Each role declares its own queue, creates the appropriate binding, then enters
 * a consume loop. All roles ack manually and set prefetch=1 for fair dispatch.
 */

/**
 * Connects to RabbitMQ with up to 10 retries (2 s between attempts).
 * Returns an open channel ready for queue/exchange declarations.
 * @param url AMQP connection URL.
 */
async function connectWithRetry(url: string): Promise<amqp.Channel> {
  for (let attempt = 1; attempt <= 10; attempt++) {
    try {
      const connection = await amqp.connect(url)
      const channel = await connection.createChannel()
      return channel
    } catch (err) {
      // Broker may not be ready yet — wait 2 s before the next attempt.
      await new Promise((r) => setTimeout(r, 2000))
    }
  }
  throw new Error("Failed to connect to RabbitMQ after 10 attempts")
}

/**
 * Binds the direct queue and starts consuming.
 * Only messages whose routing key exactly equals DIRECT_ROUTING_KEY are delivered.
 * @param channel An open AMQP channel.
 */
async function runDirect(channel: amqp.Channel): Promise<void> {
  await channel.assertExchange(EXCHANGES.DIRECT, ExchangeKind.Direct, { durable: true })
  await channel.assertQueue(QUEUES.DIRECT, { durable: true })
  // Direct routing: deliver only when the message routing key EXACTLY equals this binding key.
  await channel.bindQueue(QUEUES.DIRECT, EXCHANGES.DIRECT, DIRECT_ROUTING_KEY)
  await channel.prefetch(1)
  await channel.consume(QUEUES.DIRECT, (msg) => {
    if (!msg) return
    console.log(`[DIRECT][rk=${msg.fields.routingKey}] ${msg.content.toString()}`)
    // Ack AFTER processing: an un-acked message is redelivered if the consumer crashes.
    channel.ack(msg)
  })
}

/**
 * Binds the given fanout queue and starts consuming.
 * Fanout exchanges ignore the routing key, so every bound queue receives a copy.
 * @param channel An open AMQP channel.
 * @param instance Label for the log line ("a" or "b").
 * @param queue The queue name unique to this instance.
 */
async function runFanout(channel: amqp.Channel, instance: string, queue: string): Promise<void> {
  await channel.assertExchange(EXCHANGES.FANOUT, ExchangeKind.Fanout, { durable: true })
  await channel.assertQueue(queue, { durable: true })
  // Fanout ignores the routing key, so bind with an empty key "".
  await channel.bindQueue(queue, EXCHANGES.FANOUT, "")
  await channel.prefetch(1)
  await channel.consume(queue, (msg) => {
    if (!msg) return
    console.log(`[FANOUT][${instance}] ${msg.content.toString()}`)
    channel.ack(msg)
  })
}

/**
 * Binds the topic queue with pattern TOPIC_PATTERN and starts consuming.
 * Only messages whose routing key matches the pattern are delivered.
 * @param channel An open AMQP channel.
 */
async function runTopic(channel: amqp.Channel): Promise<void> {
  await channel.assertExchange(EXCHANGES.TOPIC, ExchangeKind.Topic, { durable: true })
  await channel.assertQueue(QUEUES.TOPIC, { durable: true })
  // Topic pattern: "*" = exactly one token, "#" = zero or more tokens. "logs.#" matches any key under "logs.".
  await channel.bindQueue(QUEUES.TOPIC, EXCHANGES.TOPIC, TOPIC_PATTERN)
  await channel.prefetch(1)
  await channel.consume(QUEUES.TOPIC, (msg) => {
    if (!msg) return
    console.log(`[TOPIC][rk=${msg.fields.routingKey}] ${msg.content.toString()}`)
    channel.ack(msg)
  })
}

async function main(): Promise<void> {
  const url = process.env.RABBITMQ_URL ?? "amqp://starci:Cuong123_A@rabbitmq:5672"
  const role = process.env.ROLE ?? "direct"
  const channel = await connectWithRetry(url)

  switch (role) {
    case "direct":
      await runDirect(channel)
      break
    case "fanout-a":
      await runFanout(channel, "a", QUEUES.FANOUT_A)
      break
    case "fanout-b":
      await runFanout(channel, "b", QUEUES.FANOUT_B)
      break
    case "topic":
      await runTopic(channel)
      break
    default:
      throw new Error(`Unknown ROLE: ${role}`)
  }
  console.log(`[consumer-${role}] bound and waiting for messages`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
