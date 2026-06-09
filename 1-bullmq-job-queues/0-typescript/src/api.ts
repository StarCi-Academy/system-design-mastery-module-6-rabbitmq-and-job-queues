import express from "express"
import type { Request, Response } from "express"
import { loadConfig } from "./config"
import { createConnection } from "./redis"
import { EmailQueueService } from "./queue"

// HTTP surface shared by all 4 languages. Every endpoint returns HTTP 200 on
// success; the API only writes jobs into Redis and returns the queue receipt.
export async function startApi(): Promise<void> {
  const cfg = loadConfig()
  const connection = createConnection()
  const service = new EmailQueueService(connection)

  const app = express()
  app.use(express.json())

  app.get("/health", (_req: Request, res: Response) => {
    res.status(200).json({ status: "ok" })
  })

  app.get("/stats", async (_req: Request, res: Response) => {
    res.status(200).json(await service.stats())
  })

  app.get("/dlq", async (_req: Request, res: Response) => {
    res.status(200).json(await service.dlq())
  })

  app.post("/jobs", async (req: Request, res: Response) => {
    const { to, subject, forceFail } = req.body as {
      to?: string
      subject?: string
      forceFail?: boolean
    }
    if (!to) {
      res.status(400).json({ error: "missing 'to'" })
      return
    }
    res.status(200).json(await service.enqueue({ to, subject, forceFail }))
  })

  app.post("/jobs/priority", async (req: Request, res: Response) => {
    const { to, priority } = req.body as { to?: string; priority?: number }
    if (!to || priority === undefined) {
      res.status(400).json({ error: "missing 'to' or 'priority'" })
      return
    }
    res.status(200).json(await service.enqueuePriority({ to }, priority))
  })

  app.post("/jobs/delayed", async (req: Request, res: Response) => {
    const { to, delayMs } = req.body as { to?: string; delayMs?: number }
    if (!to || delayMs === undefined) {
      res.status(400).json({ error: "missing 'to' or 'delayMs'" })
      return
    }
    res.status(200).json(await service.enqueueDelayed({ to }, delayMs))
  })

  app.listen(cfg.port, () => {
    // eslint-disable-next-line no-console
    console.log(`job-api listening on port ${cfg.port}`)
  })
}
