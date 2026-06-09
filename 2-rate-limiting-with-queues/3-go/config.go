package main

import (
	"os"
	"strconv"
)

// Config holds all tunables read once at startup from the environment.
// Demo defaults are safe; production overrides come from the environment.
type Config struct {
	RedisAddr         string
	Port              string
	App               string
	RateCapacity      int
	RateRefillPerSec  int
	WorkerLimitMax    int
	WorkerLimitWindow int // milliseconds
}

func loadConfig() Config {
	return Config{
		RedisAddr:         envStr("REDIS_ADDR", "localhost:6379"),
		Port:              envStr("PORT", "3000"),
		App:               envStr("APP", "api"),
		RateCapacity:      envInt("RATE_CAPACITY", 5),
		RateRefillPerSec:  envInt("RATE_REFILL_PER_SEC", 5),
		WorkerLimitMax:    envInt("LIMIT_MAX", 10),
		WorkerLimitWindow: envInt("LIMIT_DURATION_MS", 1000),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
