package com.starci.rabbitmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot entry point.
 * The active role (producer or consumer) is driven by the {@code app.role}
 * property, which is set per-service in compose.yaml via environment variables.
 */
@SpringBootApplication
public class Application {

    /**
     * Starts the Spring context; compose sets app.role to select which beans
     * ({@link PublishController} or {@link Consumers}) become active.
     *
     * @param args command-line arguments (unused; config comes from env/YAML)
     */
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
