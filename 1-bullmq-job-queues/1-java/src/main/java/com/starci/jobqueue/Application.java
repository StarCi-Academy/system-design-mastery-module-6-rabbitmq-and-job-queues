package com.starci.jobqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// Redis-backed job queue in Java using JobRunr (Redis/Lettuce storage).
// One jar, two roles selected by APP: "api" (job-api) or "worker".
// The HTTP contract is identical across all 4 languages of this lesson.
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
