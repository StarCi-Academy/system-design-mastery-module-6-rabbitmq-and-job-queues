import { Queue } from "bullmq"
import type { AppConfig } from "./config"
import { QUEUE_NAME } from "./config"

// Build a BullMQ queue handle backed by the shared Redis instance.
export function buildQueue(config: AppConfig): Queue {
  return new Queue(QUEUE_NAME, {
    connection: { host: config.redisHost, port: config.redisPort },
  })
}
