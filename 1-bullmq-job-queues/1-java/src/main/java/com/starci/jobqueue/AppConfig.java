package com.starci.jobqueue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.nosql.redis.LettuceRedisStorageProvider;
import org.jobrunr.utils.mapper.jackson.JacksonJsonMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Wires the Redis-backed StorageProvider for JobRunr plus a shared Lettuce
// connection that the DLQ list and stats counters reuse.
@Configuration
public class AppConfig {

    @Value("${REDIS_HOST:localhost}")
    private String redisHost;

    @Value("${REDIS_PORT:6379}")
    private int redisPort;

    @Bean
    public RedisClient redisClient() {
        return RedisClient.create(RedisURI.builder().withHost(redisHost).withPort(redisPort).build());
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
        return client.connect();
    }

    @Bean
    public StorageProvider storageProvider(RedisClient client) {
        // The StorageProvider must be given a JobMapper before the JobScheduler
        // uses it to serialize jobs; without this the enqueue path NPEs because
        // the provider's internal jobMapper is null.
        LettuceRedisStorageProvider provider = new LettuceRedisStorageProvider(client);
        provider.setJobMapper(new JobMapper(new JacksonJsonMapper()));
        return provider;
    }
}
