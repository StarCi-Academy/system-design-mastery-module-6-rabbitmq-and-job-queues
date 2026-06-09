import { Queue, Worker } from "bullmq"
import type { Job } from "bullmq"
import { QUEUES, loadConfig } from "./config"
import { createConnection } from "./redis"

// The worker is an independent process: it connects to the same Redis, pops
// jobs from the email queue, and throws to trigger the queue's retry machinery.
export function startWorker(): void {
  const cfg = loadConfig()
  const connection = createConnection()
  const dlqQueue = new Queue(QUEUES.DLQ, { connection })

  const worker = new Worker(
    QUEUES.EMAIL,
    async (job: Job): Promise<{ ok: true }> => {
      // eslint-disable-next-line no-console
      console.log(`Processing job ${job.id} attempt=${job.attemptsMade + 1}/${cfg.attempts}`)
      // Fixed processing delay + random failure to demo retries.
      await new Promise((resolve) => setTimeout(resolve, cfg.processDelayMs))
      const forceFail = (job.data as { forceFail?: boolean }).forceFail === true
      if (forceFail || Math.random() < cfg.failRate) {
        // Throwing makes the queue increment attemptsMade and re-enqueue with backoff.
        throw new Error(`Simulated SMTP failure for job ${job.id}`)
      }
      const to = (job.data as { to?: string }).to ?? "unknown"
      // eslint-disable-next-line no-console
      console.log(`Job ${job.id} completed: sent email to ${to}`)
      return { ok: true }
    },
    { connection, concurrency: cfg.concurrency },
  )

  worker.on("failed", async (job: Job | undefined, err: Error) => {
    if (!job) {
      return
    }
    const max = job.opts.attempts ?? 1
    // eslint-disable-next-line no-console
    console.log(`Job ${job.id} failed (attempt ${job.attemptsMade}/${max}): ${err.message}`)
    // Only when all attempts are exhausted do we move the job to the dead-letter queue.
    if (job.attemptsMade >= max) {
      await dlqQueue.add("dead", {
        data: job.data,
        failedReason: err.message,
      })
      // eslint-disable-next-line no-console
      console.log(`Job ${job.id} exhausted retries -> pushed to DLQ`)
    }
  })

  // eslint-disable-next-line no-console
  console.log(`worker listening on queue ${QUEUES.EMAIL} (concurrency=${cfg.concurrency})`)
}
