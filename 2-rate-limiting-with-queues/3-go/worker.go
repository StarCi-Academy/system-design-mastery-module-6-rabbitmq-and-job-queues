package main

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	queueKey     = "q:tasks"
	completedKey = "q:tasks:completed"
)

// runWorker drains the Redis list queue with a leaky-bucket pull loop: at most
// LIMIT_MAX jobs per window, then sleep — a shared, Redis-backed throttle.
func runWorker(ctx context.Context, rdb *redis.Client, cfg Config) {
	window := time.Duration(cfg.WorkerLimitWindow) * time.Millisecond
	log.Printf("worker started: limiter %d/%dms", cfg.WorkerLimitMax, cfg.WorkerLimitWindow)

	for {
		start := time.Now()
		for i := 0; i < cfg.WorkerLimitMax; i++ {
			// Atomically move one job from the queue; empty list => break.
			job, err := rdb.LPop(ctx, queueKey).Result()
			if err == redis.Nil {
				break
			}
			if err != nil {
				log.Printf("worker lpop error: %v", err)
				break
			}
			// Process the job (simulated unit of downstream work).
			rdb.Incr(ctx, completedKey)
			log.Printf("job %s completed at %s", job, time.Now().Format(time.RFC3339Nano))
		}
		// Pace to the start of the next window.
		if elapsed := time.Since(start); elapsed < window {
			time.Sleep(window - elapsed)
		}
	}
}
