package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := loadConfig()
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	if cfg.App == "worker" {
		runWorker(ctx, rdb, cfg)
		return
	}

	limiter := newRateLimiter(rdb, cfg)
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})

	// Per-client token-bucket check; 200 when allowed, 429 when limited.
	mux.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		clientKey := r.Header.Get("X-Client-Id")
		if clientKey == "" {
			clientKey = "anon"
		}
		res, err := limiter.CheckRate(ctx, clientKey)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if !res.Allowed {
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"message":      "Too Many Requests",
				"retryAfterMs": res.RetryAfterMs,
				"remaining":    res.Remaining,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"data": map[string]any{"value": rand.Float64(), "at": time.Now().UTC().Format(time.RFC3339Nano)},
			"rate": map[string]any{"remaining": res.Remaining},
		})
	})

	// Enqueue a batch of jobs onto the shared Redis list queue.
	mux.HandleFunc("/jobs/batch", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Count int `json:"count"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		for i := 0; i < body.Count; i++ {
			rdb.RPush(ctx, queueKey, strconv.Itoa(i))
		}
		writeJSON(w, http.StatusOK, map[string]any{"enqueued": body.Count})
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		waiting, _ := rdb.LLen(ctx, queueKey).Result()
		completed, _ := rdb.Get(ctx, completedKey).Int64()
		writeJSON(w, http.StatusOK, map[string]any{
			"waiting":   waiting,
			"active":    0,
			"completed": completed,
		})
	})

	log.Printf("rate-api listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
