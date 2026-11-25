package com.fortune.core.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fortune.core.common.ApiException;
import com.fortune.core.common.RedisKeys;
import com.fortune.core.config.FortuneProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 결제 멱등성 2중 방어.
 *
 * <ol>
 *   <li><b>Redis</b> — {@code SET idem:lock:{key} {token} NX EX ttl} 로 동시 요청을 한 건만 통과시키고,
 *       완료된 응답은 {@code idem:resp:{key}} 해시에 보관해 즉시 재생한다.</li>
 *   <li><b>PostgreSQL</b> — {@code idempotency_keys} 의 PK(=idempotency_key) + UNIQUE 로
 *       Redis 가 유실/분할되어도 이중 결제를 막는다.</li>
 * </ol>
 *
 * <p>락 해제는 반드시 <b>내 토큰일 때만</b> 지운다(compare-and-delete). 그래야 내 처리 시간이 길어져
 * 락 TTL 이 만료된 뒤 다른 요청이 새로 잡은 락을 내가 지워버리는 사고를 막는다.
 *
 * <p>Redis 쓰기는 트랜잭션 커밋 이후(afterCommit)에 한다. DB 가 롤백됐는데 캐시에 완료 응답이 남는
 * 불일치를 막기 위해서다.
 */
@Service
public class IdempotencyService {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyService.class);
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z0-9._:-]{8,128}");
    private static final String FIELD_HASH = "requestHash";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_BODY = "body";

    private static final DefaultRedisScript<Long> RELEASE_LOCK = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private final IdempotencyRepository repository;
    private final FortuneProperties properties;
    private final ObjectMapper objectMapper;

    public IdempotencyService(StringRedisTemplate redis, IdempotencyRepository repository,
                              FortuneProperties properties, ObjectMapper objectMapper) {
        this.redis = redis;
        this.repository = repository;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    // ------------------------------------------------------------------
    // 1단계: 처리 주체 결정
    // ------------------------------------------------------------------
    public IdempotencyDecision begin(String rawKey, String requestHash) {
        String key = normaliseKey(rawKey);

        Optional<CachedResponse> cached = readCache(key);
        if (cached.isPresent()) {
            CachedResponse response = cached.get();
            assertSamePayload(key, response.requestHash(), requestHash);
            log.debug("idempotent replay from redis key={}", key);
            return new IdempotencyDecision.Replay(response.status(), response.body());
        }

        String lockToken = UUID.randomUUID().toString();
        Boolean locked = acquireLock(key, lockToken);
        if (!Boolean.TRUE.equals(locked)) {
            // 다른 인스턴스(또는 다른 스레드)가 처리 중이다.
            return resolveWithoutLock(key, requestHash);
        }

        try {
            Optional<IdempotencyRecord> existing = repository.findById(key);
            if (existing.isPresent()) {
                IdempotencyRecord record = existing.get();
                assertSamePayload(key, record.getRequestHash(), requestHash);
                if (record.getStatus() == IdempotencyStatus.COMPLETED) {
                    warmCache(record);
                    releaseLock(key, lockToken);
                    log.debug("idempotent replay from db key={}", key);
                    return new IdempotencyDecision.Replay(record.getResponseStatus(), record.getResponseBody());
                }
                // IN_PROGRESS 인데 락을 내가 잡았다 = 이전 처리자가 락 TTL 만료 후 사라졌다 → 이어서 처리.
                log.warn("taking over stale IN_PROGRESS idempotency record key={}", key);
                return new IdempotencyDecision.Proceed(key, lockToken, requestHash);
            }
            repository.saveAndFlush(IdempotencyRecord.inProgress(key, requestHash, ttl()));
            return new IdempotencyDecision.Proceed(key, lockToken, requestHash);
        } catch (DataIntegrityViolationException ex) {
            // Redis 락이 만료된 사이 다른 노드가 먼저 키를 선점한 희귀 케이스. PK 중복 = 이미 처리 중.
            releaseLock(key, lockToken);
            log.warn("idempotency key unique conflict key={} cause={}", key, ex.getMostSpecificCause().getMessage());
            throw inProgress();
        } catch (RuntimeException ex) {
            releaseLock(key, lockToken);
            throw ex;
        }
    }

    // ------------------------------------------------------------------
    // 2단계: 완료 / 중단
    // ------------------------------------------------------------------
    public void complete(IdempotencyDecision.Proceed handle, int responseStatus, Object body) {
        String json = serialise(body);
        IdempotencyRecord record = repository.findById(handle.key())
                .orElseGet(() -> IdempotencyRecord.inProgress(handle.key(), handle.requestHash(), ttl()));
        record.markCompleted(responseStatus, json, ttl());
        repository.save(record);

        Runnable publish = () -> {
            try {
                writeCache(handle.key(), handle.requestHash(), responseStatus, json);
            } catch (RuntimeException ex) {
                // 캐시 실패는 치명적이지 않다: DB 기록이 남아 다음 재요청이 DB 에서 재생된다.
                log.warn("failed to cache idempotent response key={}: {}", handle.key(), ex.getMessage());
            } finally {
                releaseLock(handle.key(), handle.lockToken());
            }
        };

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    publish.run();
                }

                @Override
                public void afterCompletion(int status) {
                    if (status != TransactionSynchronization.STATUS_COMMITTED) {
                        releaseLock(handle.key(), handle.lockToken());
                    }
                }
            });
        } else {
            publish.run();
        }
    }

    /** 처리 실패(롤백) 시 락만 반납한다. DB 기록은 트랜잭션 롤백으로 사라지므로 같은 키로 재시도할 수 있다. */
    public void abort(IdempotencyDecision.Proceed handle) {
        releaseLock(handle.key(), handle.lockToken());
    }

    // ------------------------------------------------------------------
    // internal
    // ------------------------------------------------------------------
    private IdempotencyDecision resolveWithoutLock(String key, String requestHash) {
        Optional<IdempotencyRecord> existing = repository.findById(key);
        if (existing.isEmpty()) {
            throw inProgress();
        }
        IdempotencyRecord record = existing.get();
        assertSamePayload(key, record.getRequestHash(), requestHash);
        if (record.getStatus() == IdempotencyStatus.COMPLETED) {
            warmCache(record);
            return new IdempotencyDecision.Replay(record.getResponseStatus(), record.getResponseBody());
        }
        throw inProgress();
    }

    private Boolean acquireLock(String key, String lockToken) {
        return redis.opsForValue().setIfAbsent(RedisKeys.idempotencyLock(key), lockToken, properties.getIdempotency().getLockTtl());
    }

    private void releaseLock(String key, String lockToken) {
        try {
            redis.execute(RELEASE_LOCK, java.util.List.of(RedisKeys.idempotencyLock(key)), lockToken);
        } catch (RuntimeException ex) {
            log.warn("failed to release idempotency lock key={}: {} (TTL 로 회수됩니다)", key, ex.getMessage());
        }
    }

    private Optional<CachedResponse> readCache(String key) {
        Map<Object, Object> entries = redis.opsForHash().entries(RedisKeys.idempotencyResponse(key));
        if (entries.isEmpty()) {
            return Optional.empty();
        }
        Object hash = entries.get(FIELD_HASH);
        Object status = entries.get(FIELD_STATUS);
        Object body = entries.get(FIELD_BODY);
        if (hash == null || status == null || body == null) {
            return Optional.empty();
        }
        return Optional.of(new CachedResponse(hash.toString(), Integer.parseInt(status.toString()), body.toString()));
    }

    private void writeCache(String key, String requestHash, int status, String body) {
        String cacheKey = RedisKeys.idempotencyResponse(key);
        redis.opsForHash().putAll(cacheKey, Map.of(
                FIELD_HASH, requestHash,
                FIELD_STATUS, String.valueOf(status),
                FIELD_BODY, body));
        redis.expire(cacheKey, ttl());
    }

    private void warmCache(IdempotencyRecord record) {
        try {
            Duration remaining = Duration.between(Instant.now(), record.getExpiresAt());
            Duration cacheTtl = remaining.isNegative() || remaining.isZero() ? ttl() : remaining;
            String cacheKey = RedisKeys.idempotencyResponse(record.getIdempotencyKey());
            redis.opsForHash().putAll(cacheKey, Map.of(
                    FIELD_HASH, record.getRequestHash(),
                    FIELD_STATUS, String.valueOf(record.getResponseStatus()),
                    FIELD_BODY, record.getResponseBody()));
            redis.expire(cacheKey, cacheTtl);
        } catch (RuntimeException ex) {
            log.warn("failed to warm idempotency cache key={}: {}", record.getIdempotencyKey(), ex.getMessage());
        }
    }

    private void assertSamePayload(String key, String storedHash, String requestHash) {
        if (storedHash == null || !storedHash.equals(requestHash)) {
            throw ApiException.unprocessable("IDEMPOTENCY_KEY_REUSED",
                    "같은 Idempotency-Key 로 다른 내용의 요청이 들어왔습니다. 새 키를 사용해 주세요.");
        }
    }

    private String normaliseKey(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            throw ApiException.badRequest("IDEMPOTENCY_KEY_REQUIRED",
                    "Idempotency-Key 헤더가 필요합니다. 요청 1건당 고유한 값을 사용하세요.");
        }
        String key = rawKey.trim();
        if (key.length() > properties.getIdempotency().getKeyMaxLength() || !KEY_PATTERN.matcher(key).matches()) {
            throw ApiException.badRequest("IDEMPOTENCY_KEY_INVALID",
                    "Idempotency-Key 는 영문/숫자/._:- 8~128자여야 합니다.");
        }
        return key;
    }

    private ApiException inProgress() {
        return ApiException.conflict("IDEMPOTENCY_IN_PROGRESS",
                "동일한 Idempotency-Key 요청이 처리 중입니다. 잠시 후 다시 조회해 주세요.");
    }

    private String serialise(Object body) {
        try {
            return objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("응답 직렬화 실패", e);
        }
    }

    private Duration ttl() {
        return properties.getIdempotency().getRecordTtl();
    }

    private record CachedResponse(String requestHash, int status, String body) {
    }
}
