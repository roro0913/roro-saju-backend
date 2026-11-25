package com.fortune.core.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * 트랜잭션 아웃박스.
 *
 * <p>결제 확정 트랜잭션에서 이벤트를 <b>같은 DB 트랜잭션</b>으로 저장하고,
 * 별도 릴레이가 Kafka 로 내보낸다. 이렇게 하면
 * <ul>
 *   <li>DB 커밋 성공 + Kafka 발행 실패 → 이벤트가 남아 재발행된다(유실 없음)</li>
 *   <li>Kafka 발행 성공 + DB 롤백 → 발생하지 않는다(같은 커밋이므로)</li>
 *   <li>Kafka 가 느려도 결제 API 응답 시간이 늘지 않는다(완전 분리)</li>
 * </ul>
 *
 * <p>파티션이 여러 개로 커지면 같은 패턴을 Debezium CDC 로 대체하는 편이 낫다(README 참고).
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private OutboxStatus status;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_error", length = 500)
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
        // JPA
    }

    private OutboxEvent(String aggregateId, String topic, String eventType, String payload) {
        this.id = UUID.randomUUID();
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.eventType = eventType;
        this.payload = payload;
        this.status = OutboxStatus.PENDING;
        this.attempts = 0;
        this.createdAt = Instant.now();
        this.nextAttemptAt = this.createdAt;
    }

    public static OutboxEvent pending(String aggregateId, String topic, String eventType, String payload) {
        return new OutboxEvent(aggregateId, topic, eventType, payload);
    }

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
        this.lastError = null;
    }

    /** 발행 실패: 지수 백오프로 다음 시각을 미루고, 최대 횟수를 넘으면 FAILED 로 남긴다(수동 재처리 대상). */
    public void markAttemptFailed(String error, int maxAttempts, Duration baseBackoff) {
        this.attempts += 1;
        this.lastError = truncate(error, 500);
        long multiplier = 1L << Math.min(this.attempts - 1, 10);
        Duration delay = baseBackoff.multipliedBy(multiplier);
        Duration cap = Duration.ofMinutes(5);
        if (delay.compareTo(cap) > 0) {
            delay = cap;
        }
        this.nextAttemptAt = Instant.now().plus(delay);
        this.status = this.attempts >= maxAttempts ? OutboxStatus.FAILED : OutboxStatus.PENDING;
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
