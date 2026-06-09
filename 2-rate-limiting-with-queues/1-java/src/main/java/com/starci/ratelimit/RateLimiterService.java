package com.starci.ratelimit;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

/**
 * Edge-layer token bucket. The whole check-then-decrement runs as ONE atomic
 * Lua script in Redis (EVALSHA via Lettuce), so concurrent requests cannot
 * interleave.
 */
@Service
public class RateLimiterService {

    public record RateCheckResult(boolean allowed, long remaining, long retryAfterMs) {}

    private final StringRedisTemplate redis;
    private final AppProperties props;
    private final RedisScript<List> tokenBucket;

    public RateLimiterService(StringRedisTemplate redis, AppProperties props) throws Exception {
        this.redis = redis;
        this.props = props;
        String src = StreamUtils.copyToString(
                new ClassPathResource("token-bucket.lua").getInputStream(),
                StandardCharsets.UTF_8);
        this.tokenBucket = RedisScript.of(src, List.class);
    }

    public RateCheckResult checkRate(String clientKey) {
        long now = System.currentTimeMillis();
        String key = "rl:" + clientKey;

        @SuppressWarnings("unchecked")
        List<Long> result = (List<Long>) redis.execute(
                tokenBucket,
                Collections.singletonList(key),
                String.valueOf(props.getRate().getCapacity()),
                String.valueOf(props.getRate().getRefillPerSec()),
                String.valueOf(now),
                "1");

        return new RateCheckResult(result.get(0) == 1L, result.get(1), result.get(2));
    }
}
