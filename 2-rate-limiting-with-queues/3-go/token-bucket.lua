-- Token bucket rate limiter (atomic via EVAL)
-- KEYS[1] = bucket key (e.g. rl:client-42)
-- ARGV[1] = capacity (max tokens)
-- ARGV[2] = refill rate (tokens per second)
-- ARGV[3] = now (ms)
-- ARGV[4] = requested tokens (usually 1)
-- Returns { allowed (0/1), remaining_tokens, retry_after_ms }
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])
local want     = tonumber(ARGV[4])

local data   = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local last   = tonumber(data[2])

-- First request for this key: start with a full bucket.
if tokens == nil then
    tokens = capacity
    last   = now_ms
end

-- Lazy refill: add tokens proportional to elapsed time, clamped to capacity.
local delta_ms  = math.max(0, now_ms - last)
local refilled  = (delta_ms / 1000.0) * refill
tokens = math.min(capacity, tokens + refilled)

local allowed = 0
local retry_after_ms = 0
if tokens >= want then
    tokens  = tokens - want
    allowed = 1
else
    -- Not enough tokens: compute when enough will have refilled.
    local deficit  = want - tokens
    retry_after_ms = math.ceil((deficit / refill) * 1000)
end

redis.call("HMSET", key, "tokens", tokens, "ts", now_ms)
redis.call("PEXPIRE", key, math.ceil((capacity / refill) * 1000 * 2))
return { allowed, math.floor(tokens), retry_after_ms }
