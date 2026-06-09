import express, { Request, Response } from "express"
import * as amqp from "amqplib"
import { EXCHANGES, ExchangeKind } from "./topology"

/**
 * The envelope returned by every publish endpoint.
 * Carries enough context for the client to correlate the HTTP response
 * with the broker message without polling the queue.
 */
interface PublishResult {
  /** AMQP exchange the message was sent to. */
  exchange: string
  /** Routing key used; empty string for fanout. */
  routingKey: string
  /** Original request body forwarded as-is. */
  payload: unknown
  /** ISO-8601 timestamp of when the publish call was made. */
  publishedAt: string
}

/**
 * Manages the single AMQP connection and channel for the producer.
 * The producer never publishes to a queue: it always sends to an EXCHANGE,
 * and the exchange type (direct/fanout/topic) decides the routing.
 */
class Producer {
  /** Persistent AMQP connection — reused across all publish calls. */
  private connection!: amqp.Connection
  /** Single channel multiplexed over the connection. */
  private channel!: amqp.Channel

  /**
   * Connects to the broker with up to 10 retries, then declares all three
   * exchanges so the topology is ready before the first HTTP request arrives.
   * @param url AMQP connection URL, e.g. amqp://user:pass@host:5672
   */
  async onModuleInit(url: string): Promise<void> {
    // App container usually starts before the broker is ready: retry on ECONNREFUSED.
    for (let attempt = 1; attempt <= 10; attempt++) {
      try {
        this.connection = await amqp.connect(url)
        this.channel = await this.connection.createChannel()
        // assertExchange is idempotent and durable so the topology survives a broker restart.
        await this.channel.assertExchange(EXCHANGES.DIRECT, ExchangeKind.Direct, { durable: true })
        await this.channel.assertExchange(EXCHANGES.FANOUT, ExchangeKind.Fanout, { durable: true })
        await this.channel.assertExchange(EXCHANGES.TOPIC, ExchangeKind.Topic, { durable: true })
        console.log("[producer-api] connected to broker, three exchanges declared")
        return
      } catch (err) {
        console.log(`[producer-api] broker not ready (attempt ${attempt}/10), retrying...`)
        await new Promise((r) => setTimeout(r, 2000))
      }
    }
    throw new Error("Failed to connect to RabbitMQ after 10 attempts")
  }

  /**
   * Publishes a message to the given exchange with the given routing key.
   * The routing algorithm (exact-match / broadcast / pattern) is decided
   * entirely by the exchange type — this method is agnostic of that.
   * @param exchange Target exchange name (declared in onModuleInit).
   * @param routingKey Hint for the broker; fanout ignores it (pass "").
   * @param payload Arbitrary JSON-serialisable body.
   * @returns Envelope with context about the published message.
   */
  publish(exchange: string, routingKey: string, payload: unknown): PublishResult {
    // publish(exchange, routingKey, content): the producer talks to an EXCHANGE, never a queue.
    this.channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(payload)))
    return { exchange, routingKey, payload, publishedAt: new Date().toISOString() }
  }
}

async function main(): Promise<void> {
  const url = process.env.RABBITMQ_URL ?? "amqp://starci:Cuong123_A@rabbitmq:5672"
  const port = Number(process.env.PORT ?? 3000)

  const producer = new Producer()
  await producer.onModuleInit(url)

  const app = express()
  app.use(express.json())

  app.get("/health", (_req: Request, res: Response) => {
    res.status(200).json({ status: "ok" })
  })

  app.post("/publish/direct/:routingKey", (req: Request, res: Response) => {
    const result = producer.publish(EXCHANGES.DIRECT, req.params.routingKey, req.body)
    res.status(200).json(result)
  })

  app.post("/publish/fanout", (req: Request, res: Response) => {
    // Fanout ignores the routing key, so publish with an empty key "".
    const result = producer.publish(EXCHANGES.FANOUT, "", req.body)
    res.status(200).json(result)
  })

  app.post("/publish/topic/:routingKey", (req: Request, res: Response) => {
    const result = producer.publish(EXCHANGES.TOPIC, req.params.routingKey, req.body)
    res.status(200).json(result)
  })

  app.listen(port, () => console.log(`[producer-api] listening on :${port}`))
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
