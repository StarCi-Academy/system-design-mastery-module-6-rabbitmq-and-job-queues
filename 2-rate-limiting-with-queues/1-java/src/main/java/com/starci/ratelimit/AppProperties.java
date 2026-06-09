package com.starci.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Typed configuration read from application.yml (no scattered env reads). */
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private String role = "api";
    private Rate rate = new Rate();
    private Worker worker = new Worker();

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public Rate getRate() { return rate; }
    public void setRate(Rate rate) { this.rate = rate; }
    public Worker getWorker() { return worker; }
    public void setWorker(Worker worker) { this.worker = worker; }

    public static class Rate {
        private int capacity = 5;
        private int refillPerSec = 5;
        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public int getRefillPerSec() { return refillPerSec; }
        public void setRefillPerSec(int refillPerSec) { this.refillPerSec = refillPerSec; }
    }

    public static class Worker {
        private int limitMax = 10;
        private int limitDurationMs = 1000;
        public int getLimitMax() { return limitMax; }
        public void setLimitMax(int limitMax) { this.limitMax = limitMax; }
        public int getLimitDurationMs() { return limitDurationMs; }
        public void setLimitDurationMs(int limitDurationMs) { this.limitDurationMs = limitDurationMs; }
    }
}
