package com.fortune.core.ratelimit;

import com.fortune.core.common.RedisKeys;
import com.fortune.core.config.FortuneProperties;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis 슬라이딩 윈도우 레이트 리미터.
 *
 * <p>고정 윈도우 카운터(INCR+EXPIRE)는 경계 시점에 2배 버스트가 생긴다(1초 끝+다음 1초 시작).
 * 여기서는 ZSET 에 요청 시각을 쌓고 {@code now-window} 이전을 지우는 슬라이딩 윈도우를 쓴다.
 * Lua 로 묶어 <b>조회-판정-기록이 원자적</b>이다(다중 인스턴스에서도 합산 기준이 정확).
 *
 * <p>반환은 문자열 {@code "allowed:remaining:retryAfterMs"} — StringRedisTemplate 과 타입 충돌 없이
 * 한 번의 왕복으로 필요한 값을 모두 받기 위한 선택이다.
 */
@Component
public class RedisSlidingWindowRateLimiter {

    private static final String SCRIPT = """
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local window = tonumber(ARGV[2])
            local limit = tonumber(ARGV[3])
            local member = ARGV[4]
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            local count = redis.call('ZCARD', key)
            if count < limit then
              redis.call('ZADD', key, now, member)
              redis.call('PEXPIRE', key, window + 1000)
              return '1:' .. (limit - count - 1) .. ':0'
            end
            local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
            local retry = 0
            if oldest[2] then
              retry = math.ceil((tonumber(oldest[2]) + window) - now)
              if retry < 0 then retry = 0 end
            end
            return '0:0:' .. retry
            """;

    private static final DefaultRedisScript<String> RATE_LIMIT_SCRIPT =
            new DefaultRedisScript<>(SCRIPT, String.class);

    private final StringRedisTemplate redis;
    private final FortuneProperties properties;

    public RedisSlidingWindowRateLimiter(StringRedisTemplate redis, FortuneProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    public RateLimitDecision tryAcquire(String scope, String clientKey) {
        FortuneProperties.RateLimit config = properties.getRateLimit();
        Duration window = config.getWindow();
        long now = System.currentTimeMillis();
        String member = now + "-" + UUID.randomUUID();

        String raw = redis.execute(
                RATE_LIMIT_SCRIPT,
                List.of(RedisKeys.rateLimit(scope, clientKey)),
                String.valueOf(now),
                String.valueOf(window.toMillis()),
                String.valueOf(config.getRequestsPerWindow()),
                member);

        if (raw == null) {
            // 스크립트 결과가 없으면 보수적으로 통과(가용성 우선). 요청 자체를 막지는 않는다.
            return new RateLimitDecision(true, config.getRequestsPerWindow(), config.getRequestsPerWindow(), 0);
        }
        String[] parts = raw.split(":");
        boolean allowed = parts.length > 0 && "1".equals(parts[0]);
        long remaining = parts.length > 1 ? Long.parseLong(parts[1]) : 0;
        long retryAfterMillis = parts.length > 2 ? Long.parseLong(parts[2]) : 0;
        return new RateLimitDecision(allowed, config.getRequestsPerWindow(), remaining, retryAfterMillis);
    }

    public record RateLimitDecision(boolean allowed, long limit, long remaining, long retryAfterMillis) {
    }
}
