import { Worker } from "bullmq"
import { loadConfig, QUEUE_NAME } from "./config"

// Worker-layer limiter: a shared Redis-backed BullMQ limiter caps processing
// at LIMIT_MAX jobs per LIMIT_DURATION_MS across every worker process.
export function startWorker(): void {
  const config = loadConfig()

  const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
      // Simulate a small unit of downstream work.
      return { processed: job.id }
    },
    {
      connection: { host: config.redisHost, port: config.redisPort },
      limiter: {
        max: config.workerLimitMax,
        duration: config.workerLimitDurationMs,
      },
    },
  )

  worker.on("completed", (job) => {
    // eslint-disable-next-line no-console
    console.log(`job ${job.id} completed at ${new Date().toISOString()}`)
  })

  // eslint-disable-next-line no-console
  console.log(
    `worker started: limiter ${config.workerLimitMax}/${config.workerLimitDurationMs}ms`,
  )
}
