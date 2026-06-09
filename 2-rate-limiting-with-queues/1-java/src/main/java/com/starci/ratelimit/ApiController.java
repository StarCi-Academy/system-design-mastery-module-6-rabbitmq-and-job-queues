package com.starci.ratelimit;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/** Edge API: token-bucket gate on /api/data, batch enqueue, and queue stats. */
@RestController
public class ApiController {

    static final String QUEUE_KEY = "q:tasks";
    static final String COMPLETED_KEY = "q:tasks:completed";

    private final RateLimiterService limiter;
    private final StringRedisTemplate redis;

    public ApiController(RateLimiterService limiter, StringRedisTemplate redis) {
        this.limiter = limiter;
        this.redis = redis;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    // Per-client token-bucket check; 200 when allowed, 429 when limited.
    @GetMapping("/api/data")
    public ResponseEntity<Map<String, Object>> getData(
            @RequestHeader(value = "X-Client-Id", required = false) String clientId) {
        String clientKey = (clientId == null || clientId.isEmpty()) ? "anon" : clientId;
        RateLimiterService.RateCheckResult r = limiter.checkRate(clientKey);
        if (!r.allowed()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(Map.of(
                    "message", "Too Many Requests",
                    "retryAfterMs", r.retryAfterMs(),
                    "remaining", r.remaining()));
        }
        return ResponseEntity.ok(Map.of(
                "data", Map.of("value", ThreadLocalRandom.current().nextDouble(),
                        "at", Instant.now().toString()),
                "rate", Map.of("remaining", r.remaining())));
    }

    // Enqueue a batch of jobs onto the shared Redis list queue.
    @PostMapping("/jobs/batch")
    public Map<String, Object> batch(@RequestBody Map<String, Object> body) {
        int count = ((Number) body.getOrDefault("count", 0)).intValue();
        for (int i = 0; i < count; i++) {
            redis.opsForList().rightPush(QUEUE_KEY, String.valueOf(i));
        }
        return Map.of("enqueued", count);
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Long waiting = redis.opsForList().size(QUEUE_KEY);
        String completed = redis.opsForValue().get(COMPLETED_KEY);
        return Map.of(
                "waiting", waiting == null ? 0 : waiting,
                "active", 0,
                "completed", completed == null ? 0L : Long.parseLong(completed));
    }
}
