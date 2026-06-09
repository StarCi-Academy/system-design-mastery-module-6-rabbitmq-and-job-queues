// Centralized configuration read once at process start.
// Demo defaults are safe; production overrides come from the environment.
export interface AppConfig {
  readonly redisHost: string
  readonly redisPort: number
  readonly port: number
  readonly rateCapacity: number
  readonly rateRefillPerSec: number
  readonly workerLimitMax: number
  readonly workerLimitDurationMs: number
}

export function loadConfig(): AppConfig {
  return {
    redisHost: process.env.REDIS_HOST ?? "localhost",
    redisPort: Number(process.env.REDIS_PORT ?? 6379),
    port: Number(process.env.PORT ?? 3000),
    rateCapacity: Number(process.env.RATE_CAPACITY ?? 5),
    rateRefillPerSec: Number(process.env.RATE_REFILL_PER_SEC ?? 5),
    workerLimitMax: Number(process.env.LIMIT_MAX ?? 10),
    workerLimitDurationMs: Number(process.env.LIMIT_DURATION_MS ?? 1000),
  }
}

export const QUEUE_NAME = "tasks"
