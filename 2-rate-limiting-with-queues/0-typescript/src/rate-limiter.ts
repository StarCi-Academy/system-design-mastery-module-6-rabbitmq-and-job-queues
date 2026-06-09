import { readFileSync } from "fs"
import { join } from "path"
import type { Redis } from "ioredis"
import type { AppConfig } from "./config"

export interface RateCheckResult {
  readonly allowed: boolean
  readonly remaining: number
  readonly retryAfterMs: number
}

// Edge-layer token bucket. The whole check-then-decrement runs as ONE atomic
// Lua script in Redis (EVALSHA), so concurrent requests cannot interleave.
export class RateLimiter {
  private luaSha = ""
  private readonly luaScript: string

  constructor(
    private readonly redis: Redis,
    private readonly config: AppConfig,
  ) {
    const scriptPath = join(__dirname, "lua", "token-bucket.lua")
    this.luaScript = readFileSync(scriptPath, "utf-8")
  }

  // Preload the script so EVALSHA can reuse the cached SHA on every call.
  async init(): Promise<void> {
    this.luaSha = (await this.redis.script("LOAD", this.luaScript)) as string
  }

  get scriptSha(): string {
    return this.luaSha
  }

  // One atomic round-trip: returns { allowed, remaining, retryAfterMs }.
  async checkRate(clientKey: string): Promise<RateCheckResult> {
    const now = Date.now()
    const key = `rl:${clientKey}`

    const result = (await this.redis.evalsha(
      this.luaSha,
      1,
      key,
      this.config.rateCapacity,
      this.config.rateRefillPerSec,
      now,
      1,
    )) as [number, number, number]

    return {
      allowed: result[0] === 1,
      remaining: result[1],
      retryAfterMs: result[2],
    }
  }
}
