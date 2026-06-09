# 0-rabbitmq-fundamentals

Hands-on source for the **RabbitMQ fundamentals** lesson: direct / fanout / topic
exchanges plus `ack`/`prefetch`. Four language tracks, all sharing the same HTTP
contract and exchange/queue topology.

## Tracks

| Folder | Stack |
|--------|-------|
| `0-typescript` | Node.js + Express + `amqplib` |
| `1-java` | Spring Boot + `spring-boot-starter-amqp` |
| `2-csharp` | ASP.NET Core + `RabbitMQ.Client` v7 (async) |
| `3-go` | Go `net/http` + `amqp091-go` |

## API contract (identical across tracks)

| Method | Path | Body | Response 200 |
|--------|------|------|--------------|
| GET | `/health` | — | `{ "status": "ok" }` |
| POST | `/publish/direct/:routingKey` | `{ orderId, total }` | `{ exchange, routingKey, payload, publishedAt }` |
| POST | `/publish/fanout` | `{ event, userId }` | `{ exchange, routingKey: "", payload, publishedAt }` |
| POST | `/publish/topic/:routingKey` | `{ msg }` | `{ exchange, routingKey, payload, publishedAt }` |

## Topology

| Exchange | Type | Routing | Queue(s) |
|----------|------|---------|----------|
| `orders.direct` | direct | `orders.created` | `orders.created.q` |
| `events.fanout` | fanout | `""` | `events.fanout.q.a`, `events.fanout.q.b` |
| `logs.topic` | topic | `logs.#` | `logs.topic.q` |

## Run a track

```bash
cd <N>-<lang>/.docker
docker compose up -d --build
# ... call the endpoints, read consumer logs ...
docker compose down -v
```

Broker: `rabbitmq:3.13-management` (AMQP 5672, Management UI 15672, login `starci` / `Cuong123_A`).
