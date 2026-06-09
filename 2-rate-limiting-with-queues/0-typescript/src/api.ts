import express from "express"
import Redis from "ioredis"
import type { Request, Response } from "express"
import { loadConfig } from "./config"
import { RateLimiter } from "./rate-limiter"
import { buildQueue } from "./queue"

// Edge API: token-bucket gate on /api/data, batch enqueue, and queue stats.
export async function startApi(): Promise<void> {
  const config = loadConfig()
  const redis = new Redis({ host: config.redisHost, port: config.redisPort })
  const limiter = new RateLimiter(redis, config)
  await limiter.init()
  const queue = buildQueue(config)

  const app = express()
  app.use(express.json())

  app.get("/health", (_req: Request, res: Response) => {
    res.status(200).json({ status: "ok" })
  })

  // Per-client token-bucket check; 200 when allowed, 429 when limited.
  app.get("/api/data", async (req: Request, res: Response) => {
    const clientKey =
      (req.headers["x-client-id"] as string) || req.ip || "anon"
    const result = await limiter.checkRate(clientKey)
    if (!result.allowed) {
      // Reject before the request reaches downstream services.
      res.status(429).json({
        message: "Too Many Requests",
        retryAfterMs: result.retryAfterMs,
        remaining: result.remaining,
      })
      return
    }
    res.status(200).json({
      data: { value: Math.random(), at: new Date().toISOString() },
      rate: { remaining: result.remaining },
    })
  })

  // Enqueue a batch of jobs; the worker drains them at a throttled rate.
  app.post("/jobs/batch", async (req: Request, res: Response) => {
    const count = Number(req.body?.count ?? 0)
    const jobs = Array.from({ length: count }, (_v, i) => ({
      name: "task",
      data: { index: i },
    }))
    await queue.addBulk(jobs)
    res.status(200).json({ enqueued: count })
  })

  app.get("/stats", async (_req: Request, res: Response) => {
    const counts = await queue.getJobCounts(
      "waiting",
      "active",
      "completed",
    )
    res.status(200).json({
      waiting: counts.waiting ?? 0,
      active: counts.active ?? 0,
      completed: counts.completed ?? 0,
    })
  })

  app.listen(config.port, () => {
    // eslint-disable-next-line no-console
    console.log(
      `rate-api listening on :${config.port} (lua sha=${limiter.scriptSha})`,
    )
  })
}
