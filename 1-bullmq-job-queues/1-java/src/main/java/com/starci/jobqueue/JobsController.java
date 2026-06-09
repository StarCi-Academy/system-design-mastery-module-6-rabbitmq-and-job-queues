package com.starci.jobqueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.api.StatefulRedisConnection;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.StorageProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// HTTP surface shared by all 4 languages. The API only enqueues jobs into Redis
// and returns the queue receipt with HTTP 200 on success.
@RestController
public class JobsController {

    private final JobScheduler jobScheduler;
    private final EmailJob emailJob;
    private final StorageProvider storageProvider;
    private final StatefulRedisConnection<String, String> redis;
    private final ObjectMapper mapper = new ObjectMapper();

    public JobsController(JobScheduler jobScheduler, EmailJob emailJob,
                          StorageProvider storageProvider,
                          StatefulRedisConnection<String, String> redis) {
        this.jobScheduler = jobScheduler;
        this.emailJob = emailJob;
        this.storageProvider = storageProvider;
        this.redis = redis;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        var counters = storageProvider.getJobStats();
        Map<String, Object> email = new LinkedHashMap<>();
        email.put("waiting", counters.getEnqueued().intValue());
        email.put("active", counters.getProcessing().intValue());
        email.put("completed", counters.getSucceeded().intValue());
        email.put("failed", counters.getFailed().intValue());
        email.put("delayed", counters.getScheduled().intValue());
        long dlqLen = redis.sync().llen("dlq:email");
        return Map.of("email", email, "dlq", Map.of("waiting", (int) dlqLen));
    }

    @GetMapping("/dlq")
    public List<Map<String, Object>> dlq() throws Exception {
        List<String> items = redis.sync().lrange("dlq:email", 0, 100);
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            JsonNode node = mapper.readTree(items.get(i));
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", String.valueOf(i));
            entry.put("name", "dead");
            entry.put("data", mapper.convertValue(node.get("data"), Map.class));
            entry.put("failedReason", node.get("failedReason").asText());
            out.add(entry);
        }
        return out;
    }

    @PostMapping("/jobs")
    public ResponseEntity<?> enqueue(@RequestBody EmailRequest req) {
        if (req.to() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing 'to'"));
        }
        var id = jobScheduler.enqueue(() -> emailJob.send(req.to(), req.forceFail()));
        return ResponseEntity.ok(Map.of("id", id.toString(), "name", "send", "queue", "email"));
    }

    @PostMapping("/jobs/priority")
    public ResponseEntity<?> enqueuePriority(@RequestBody PriorityRequest req) {
        if (req.to() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing 'to' or 'priority'"));
        }
        var id = jobScheduler.enqueue(() -> emailJob.send(req.to(), false));
        return ResponseEntity.ok(Map.of("id", id.toString(), "name", "send", "priority", req.priority(), "queue", "email"));
    }

    @PostMapping("/jobs/delayed")
    public ResponseEntity<?> enqueueDelayed(@RequestBody DelayedRequest req) {
        if (req.to() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "missing 'to' or 'delayMs'"));
        }
        var id = jobScheduler.schedule(java.time.Instant.now().plus(Duration.ofMillis(req.delayMs())),
                () -> emailJob.send(req.to(), false));
        return ResponseEntity.ok(Map.of("id", id.toString(), "name", "send", "delayMs", req.delayMs(), "queue", "email"));
    }

    public record EmailRequest(String to, String subject, boolean forceFail) {
    }

    public record PriorityRequest(String to, int priority) {
    }

    public record DelayedRequest(String to, int delayMs) {
    }
}
