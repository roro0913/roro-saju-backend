package com.fortune.core.common;

import java.util.UUID;

/**
 * Redis 키 규약.
 *
 * <p><b>python-ai-worker/app/keys.py 와 동일해야 한다.</b> 한쪽만 바꾸면 조회 API 가 상태를 못 찾는다.
 */
public final class RedisKeys {

    public static final String IDEMPOTENCY_LOCK_PREFIX = "idem:lock:";
    public static final String IDEMPOTENCY_RESPONSE_PREFIX = "idem:resp:";
    public static final String RATE_LIMIT_PREFIX = "rl:sw:";
    public static final String READING_STATUS_PREFIX = "reading:status:";
    public static final String READING_RESULT_PREFIX = "reading:result:";

    private RedisKeys() {
    }

    public static String idempotencyLock(String idempotencyKey) {
        return IDEMPOTENCY_LOCK_PREFIX + idempotencyKey;
    }

    public static String idempotencyResponse(String idempotencyKey) {
        return IDEMPOTENCY_RESPONSE_PREFIX + idempotencyKey;
    }

    public static String rateLimit(String scope, String clientKey) {
        return RATE_LIMIT_PREFIX + scope + ":" + clientKey;
    }

    public static String readingStatus(UUID orderId) {
        return READING_STATUS_PREFIX + orderId;
    }

    public static String readingResult(UUID orderId) {
        return READING_RESULT_PREFIX + orderId;
    }
}
