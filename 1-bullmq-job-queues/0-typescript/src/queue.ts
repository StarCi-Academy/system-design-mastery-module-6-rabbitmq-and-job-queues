import { Queue } from "bullmq"
import type IORedis from "ioredis"
import { QUEUES, loadConfig } from "./config"

// Wraps the email queue and DLQ. The API only writes jobs into Redis and
// returns immediately — it never waits for the worker to process them.
export class EmailQueueService {
  private readonly emailQueue: Queue
  private readonly dlqQueue: Queue
  private readonly attempts: number

  constructor(connection: IORedis) {
    this.emailQueue = new Queue(QUEUES.EMAIL, { connection })
    this.dlqQueue = new Queue(QUEUES.DLQ, { connection })
    this.attempts = loadConfig().attempts
  }

  // Enqueue a normal job. Returns the queue receipt, not a processing result.
  async enqueue(payload: Record<string, unknown>): Promise<{
    id: string
    name: string
    queue: string
  }> {
    const job = await this.emailQueue.add("send", payload, {
      attempts: this.attempts,
      backoff: { type: "exponential", delay: 1000 },
    })
    return { id: String(job.id), name: job.name, queue: QUEUES.EMAIL }
  }

  // Queue.add writes the job into Redis; lower priority number is served first.
  async enqueuePriority(
    payload: Record<string, unknown>,
    priority: number,
  ): Promise<{ id: string; name: string; priority: number; queue: string }> {
    const job = await this.emailQueue.add("send", payload, {
      priority,
      attempts: this.attempts,
      backoff: { type: "exponential", delay: 1000 },
    })
    return { id: String(job.id), name: job.name, priority, queue: QUEUES.EMAIL }
  }

  // Delayed jobs sit in the delayed ZSET with score = now + delayMs.
  async enqueueDelayed(
    payload: Record<string, unknown>,
    delayMs: number,
  ): Promise<{ id: string; name: string; delayMs: number; queue: string }> {
    const job = await this.emailQueue.add("send", payload, {
      delay: delayMs,
      attempts: this.attempts,
      backoff: { type: "exponential", delay: 1000 },
    })
    return { id: String(job.id), name: job.name, delayMs, queue: QUEUES.EMAIL }
  }

  // Counters per queue state — the primary signal for "queue depth rising" alerts.
  async stats(): Promise<{
    email: Record<string, number>
    dlq: { waiting: number }
  }> {
    const counts = await this.emailQueue.getJobCounts(
      "waiting",
      "active",
      "completed",
      "failed",
      "delayed",
    )
    const dlqCounts = await this.dlqQueue.getJobCounts("waiting")
    return {
      email: {
        waiting: counts.waiting ?? 0,
        active: counts.active ?? 0,
        completed: counts.completed ?? 0,
        failed: counts.failed ?? 0,
        delayed: counts.delayed ?? 0,
      },
      dlq: { waiting: dlqCounts.waiting ?? 0 },
    }
  }

  // The DLQ has no worker, so admins read dead jobs here to inspect and replay.
  async dlq(): Promise<
    Array<{ id: string; name: string; data: unknown; failedReason: string }>
  > {
    const jobs = await this.dlqQueue.getJobs(["waiting", "completed"], 0, 100)
    return jobs.map((job) => ({
      id: String(job.id),
      name: job.name,
      data: (job.data as { data?: unknown }).data ?? job.data,
      failedReason:
        (job.data as { failedReason?: string }).failedReason ?? "unknown",
    }))
  }
}
