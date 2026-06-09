// Centralized configuration read once at process start.
// Demo defaults are safe; production overrides come from the environment.
export interface AppConfig {
  readonly redisHost: string
  readonly redisPort: number
  readonly port: number
  readonly failRate: number
  readonly processDelayMs: number
  readonly concurrency: number
  readonly attempts: number
}

export function loadConfig(): AppConfig {
  return {
    redisHost: process.env.REDIS_HOST ?? "localhost",
    redisPort: Number(process.env.REDIS_PORT ?? 6379),
    port: Number(process.env.PORT ?? 3000),
    failRate: Number(process.env.FAIL_RATE ?? 0.3),
    processDelayMs: Number(process.env.PROCESS_DELAY_MS ?? 500),
    concurrency: Number(process.env.WORKER_CONCURRENCY ?? 1),
    attempts: Number(process.env.JOB_ATTEMPTS ?? 3),
  }
}

// Queue names shared by the API and the worker process.
export const QUEUES = {
  EMAIL: "email",
  DLQ: "email-dlq",
} as const
