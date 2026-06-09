package com.starci.jobqueue;

import io.lettuce.core.api.StatefulRedisConnection;
import org.jobrunr.jobs.annotations.Job;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

// EmailJob is the JobRunr handler. Throwing makes JobRunr retry with backoff;
// the final failure records a dead job on the Redis-backed DLQ list.
@Component
public class EmailJob {

    @Value("${FAIL_RATE:0.3}")
    private double failRate;

    @Value("${PROCESS_DELAY_MS:500}")
    private long processDelayMs;

    @Value("${JOB_ATTEMPTS:3}")
    private int attempts;

    private final StatefulRedisConnection<String, String> redis;

    public EmailJob(StatefulRedisConnection<String, String> redis) {
        this.redis = redis;
    }

    @Job(name = "Send email", retries = 3)
    public void send(String to, boolean forceFail) {
        System.out.println("Processing job for " + to);
        try {
            Thread.sleep(processDelayMs);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        if (forceFail || ThreadLocalRandom.current().nextDouble() < failRate) {
            // Record into the DLQ list before throwing so the dead job survives.
            String dead = "{\"data\":{\"to\":\"" + to + "\"},\"failedReason\":\"Simulated SMTP failure for " + to + "\"}";
            redis.sync().rpush("dlq:email", dead);
            throw new IllegalStateException("Simulated SMTP failure for " + to);
        }
        System.out.println("Job completed: sent email to " + to);
    }
}
