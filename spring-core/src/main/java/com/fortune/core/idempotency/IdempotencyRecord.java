package com.fortune.core.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Duration;
import java.time.Instant;

/**
 * 결제 멱등성 기록 (DB 방어선).
 *
 * <p>Redis 락/캐시가 1차 방어선이라면 이 테이블은 <b>UNIQUE(idempotency_key)</b> 로
 * Redis 가 날아가거나 클러스터가 쪼개진 상황에서도 이중 결제를 막는 최종 방어선이다.
 */
@Entity
@Table(name = "idempotency_keys")
public class IdempotencyRecord {

    @Id
    @Column(name = "idempotency_key", nullable = false, updatable = false, length = 128)
    private String idempotencyKey;

    @Column(name = "request_hash", nullable = false, length = 64)
    private String requestHash;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private IdempotencyStatus status;

    @Column(name = "response_status")
    private Integer responseStatus;

    @Column(name = "response_body", columnDefinition = "text")
    private String responseBody;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    protected IdempotencyRecord() {
        // JPA
    }

    private IdempotencyRecord(String idempotencyKey, String requestHash, Instant now, Duration ttl) {
        this.idempotencyKey = idempotencyKey;
        this.requestHash = requestHash;
        this.status = IdempotencyStatus.IN_PROGRESS;
        this.createdAt = now;
        this.updatedAt = now;
        this.expiresAt = now.plus(ttl);
    }

    public static IdempotencyRecord inProgress(String idempotencyKey, String requestHash, Duration ttl) {
        return new IdempotencyRecord(idempotencyKey, requestHash, Instant.now(), ttl);
    }

    public void markCompleted(int responseStatus, String responseBody, Duration ttl) {
        Instant now = Instant.now();
        this.status = IdempotencyStatus.COMPLETED;
        this.responseStatus = responseStatus;
        this.responseBody = responseBody;
        this.updatedAt = now;
        this.expiresAt = now.plus(ttl);
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getRequestHash() {
        return requestHash;
    }

    public IdempotencyStatus getStatus() {
        return status;
    }

    public Integer getResponseStatus() {
        return responseStatus;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }
}
