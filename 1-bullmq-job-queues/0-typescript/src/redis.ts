import IORedis from "ioredis"
import { loadConfig } from "./config"

// A single shared ioredis connection avoids opening two sockets for the
// Queue and the Worker. maxRetriesPerRequest must be null for BullMQ.
export function createConnection(): IORedis {
  const cfg = loadConfig()
  return new IORedis({
    host: cfg.redisHost,
    port: cfg.redisPort,
    maxRetriesPerRequest: null,
  })
}
