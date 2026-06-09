package com.starci.ratelimit;

import java.time.Instant;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Worker-layer limiter: a leaky-bucket pull loop drains the shared Redis list
 * at most limitMax jobs per window, then paces — protecting downstream.
 * Only active when app.role=worker.
 */
@Component
public class ThrottledWorker {

    private final StringRedisTemplate redis;
    private final AppProperties props;

    public ThrottledWorker(StringRedisTemplate redis, AppProperties props) {
        this.redis = redis;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!"worker".equals(props.getRole())) {
            return;
        }
        int max = props.getWorker().getLimitMax();
        long window = props.getWorker().getLimitDurationMs();
        Thread t = new Thread(() -> runLoop(max, window), "throttled-worker");
        t.setDaemon(true);
        t.start();
        System.out.printf("worker started: limiter %d/%dms%n", max, window);
    }

    private void runLoop(int max, long window) {
        while (true) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < max; i++) {
                String job = redis.opsForList().leftPop(ApiController.QUEUE_KEY);
                if (job == null) {
                    break;
                }
                redis.opsForValue().increment(ApiController.COMPLETED_KEY);
                System.out.printf("job %s completed at %s%n", job, Instant.now());
            }
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed < window) {
                try {
                    Thread.sleep(window - elapsed);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
