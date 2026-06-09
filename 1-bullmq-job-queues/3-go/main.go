// Redis-backed job queue in Go using hibiken/asynq.
// One binary, two roles selected by APP: "api" (job-api) or "worker".
// The HTTP contract is identical across all 4 languages of this lesson.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

const (
	taskSendEmail = "send_email"
	queueEmail    = "email"
)

type config struct {
	redisAddr      string
	port           string
	failRate       float64
	processDelayMs int
	concurrency    int
	attempts       int
}

func loadConfig() config {
	return config{
		redisAddr:      getenv("REDIS_HOST", "localhost") + ":" + getenv("REDIS_PORT", "6379"),
		port:           getenv("PORT", "3000"),
		failRate:       getenvFloat("FAIL_RATE", 0.3),
		processDelayMs: getenvInt("PROCESS_DELAY_MS", 500),
		concurrency:    getenvInt("WORKER_CONCURRENCY", 1),
		attempts:       getenvInt("JOB_ATTEMPTS", 3),
	}
}

func main() {
	cfg := loadConfig()
	if os.Getenv("APP") == "worker" {
		runWorker(cfg)
		return
	}
	runAPI(cfg)
}

// emailPayload is the job data carried through Redis.
type emailPayload struct {
	To        string `json:"to"`
	Subject   string `json:"subject,omitempty"`
	ForceFail bool   `json:"forceFail,omitempty"`
}

// runAPI exposes the shared HTTP surface; it only writes jobs into Redis.
func runAPI(cfg config) {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.redisAddr})
	defer client.Close()
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.redisAddr})
	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		email := queueStats(inspector, queueEmail)
		// DLQ jobs are stored in a Redis list (dlq:email), not an asynq queue,
		// so read the list length directly instead of via the inspector.
		dlqLen, _ := rdb.LLen(r.Context(), "dlq:email").Result()
		writeJSON(w, http.StatusOK, map[string]any{
			"email": email,
			"dlq":   map[string]int{"waiting": int(dlqLen)},
		})
	})

	mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, deadJobs(rdb, r.Context()))
	})

	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		var p emailPayload
		if !decodeBody(w, r, &p) {
			return
		}
		info := enqueue(client, p, cfg.attempts)
		writeJSON(w, http.StatusOK, map[string]string{"id": info.ID, "name": "send", "queue": queueEmail})
	})

	mux.HandleFunc("/jobs/priority", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			To       string `json:"to"`
			Priority int    `json:"priority"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		info := enqueue(client, emailPayload{To: body.To}, cfg.attempts)
		writeJSON(w, http.StatusOK, map[string]any{"id": info.ID, "name": "send", "priority": body.Priority, "queue": queueEmail})
	})

	mux.HandleFunc("/jobs/delayed", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			To      string `json:"to"`
			DelayMs int    `json:"delayMs"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		task, _ := newTask(emailPayload{To: body.To})
		info, _ := client.Enqueue(task,
			asynq.MaxRetry(cfg.attempts-1),
			asynq.Queue(queueEmail),
			asynq.ProcessIn(time.Duration(body.DelayMs)*time.Millisecond),
		)
		writeJSON(w, http.StatusOK, map[string]any{"id": info.ID, "name": "send", "delayMs": body.DelayMs, "queue": queueEmail})
	})

	log.Printf("job-api listening on port %s", cfg.port)
	if err := http.ListenAndServe(":"+cfg.port, mux); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// runWorker pops jobs from the email queue and pushes exhausted ones to the DLQ.
func runWorker(cfg config) {
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: cfg.redisAddr},
		asynq.Config{
			Concurrency: cfg.concurrency,
			Queues:      map[string]int{queueEmail: 1},
		},
	)
	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})

	mux := asynq.NewServeMux()
	mux.HandleFunc(taskSendEmail, func(ctx context.Context, t *asynq.Task) error {
		var p emailPayload
		_ = json.Unmarshal(t.Payload(), &p)
		retried, _ := asynq.GetRetryCount(ctx)
		maxRetry, _ := asynq.GetMaxRetry(ctx)
		log.Printf("Processing job attempt=%d/%d", retried+1, maxRetry+1)
		// Fixed processing delay + random failure to demo retries.
		time.Sleep(time.Duration(cfg.processDelayMs) * time.Millisecond)
		if p.ForceFail || rand.Float64() < cfg.failRate {
			// Returning a non-nil error lets asynq run the full retry chain with
			// exponential backoff. Only on the final attempt (retried == maxRetry)
			// do we push the dead job to the Redis-backed DLQ list, then mark the
			// task done with SkipRetry so it is not retried past its budget.
			if retried >= maxRetry {
				dead, _ := json.Marshal(map[string]any{"data": p, "failedReason": "Simulated SMTP failure"})
				rdb.RPush(ctx, "dlq:email", dead)
				log.Printf("job exhausted retries -> pushed to DLQ")
				return asynq.SkipRetry
			}
			return errors.New("Simulated SMTP failure")
		}
		log.Printf("job completed: sent email to %s", p.To)
		return nil
	})

	log.Printf("worker listening on queue %s (concurrency=%d)", queueEmail, cfg.concurrency)
	if err := srv.Run(mux); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func newTask(p emailPayload) (*asynq.Task, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(taskSendEmail, b), nil
}

func enqueue(client *asynq.Client, p emailPayload, attempts int) *asynq.TaskInfo {
	task, _ := newTask(p)
	info, _ := client.Enqueue(task, asynq.MaxRetry(attempts-1), asynq.Queue(queueEmail))
	return info
}

func queueStats(inspector *asynq.Inspector, queue string) map[string]int {
	info, err := inspector.GetQueueInfo(queue)
	if err != nil {
		return map[string]int{"waiting": 0, "active": 0, "completed": 0, "failed": 0, "delayed": 0}
	}
	return map[string]int{
		"waiting":   info.Pending,
		"active":    info.Active,
		"completed": info.Completed,
		"failed":    info.Failed,
		"delayed":   info.Scheduled,
	}
}

func deadJobs(rdb *redis.Client, ctx context.Context) []map[string]any {
	out := []map[string]any{}
	items, err := rdb.LRange(ctx, "dlq:email", 0, 100).Result()
	if err != nil {
		return out
	}
	for i, raw := range items {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			m["id"] = strconv.Itoa(i)
			m["name"] = "dead"
			out = append(out, m)
		}
	}
	return out
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func getenvInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func getenvFloat(k string, d float64) float64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return d
}
