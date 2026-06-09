package main

import (
	"context"
	_ "embed"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed token-bucket.lua
var tokenBucketSrc string

// RateCheckResult is the decision returned by one atomic bucket check.
type RateCheckResult struct {
	Allowed      bool
	Remaining    int64
	RetryAfterMs int64
}

// RateLimiter wraps the token-bucket Lua script. go-redis caches the script
// SHA automatically and uses EVALSHA, falling back to EVAL on a cache miss.
type RateLimiter struct {
	rdb    *redis.Client
	script *redis.Script
	cfg    Config
}

func newRateLimiter(rdb *redis.Client, cfg Config) *RateLimiter {
	return &RateLimiter{
		rdb:    rdb,
		script: redis.NewScript(tokenBucketSrc),
		cfg:    cfg,
	}
}

// CheckRate runs the whole check-then-decrement as ONE atomic round-trip.
func (l *RateLimiter) CheckRate(ctx context.Context, clientKey string) (RateCheckResult, error) {
	now := time.Now().UnixMilli()
	key := "rl:" + clientKey

	res, err := l.script.Run(
		ctx, l.rdb, []string{key},
		l.cfg.RateCapacity, l.cfg.RateRefillPerSec, now, 1,
	).Result()
	if err != nil {
		return RateCheckResult{}, err
	}

	vals := res.([]interface{})
	return RateCheckResult{
		Allowed:      toInt64(vals[0]) == 1,
		Remaining:    toInt64(vals[1]),
		RetryAfterMs: toInt64(vals[2]),
	}, nil
}

func toInt64(v interface{}) int64 {
	if n, ok := v.(int64); ok {
		return n
	}
	return 0
}
