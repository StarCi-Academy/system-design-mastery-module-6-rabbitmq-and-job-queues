import { startApi } from "./api"
import { startWorker } from "./worker"

// One image, two roles selected by APP: "api" (edge) or "worker" (throttled).
async function bootstrap(): Promise<void> {
  const role = process.env.APP ?? "api"
  if (role === "worker") {
    startWorker()
    return
  }
  await startApi()
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error("fatal:", err)
  process.exit(1)
})
