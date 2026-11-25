package com.fortune.core.payment;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 결제 승인 기록.
 *
 * <p>{@code order_id} 와 {@code idempotency_key} 에 UNIQUE 제약이 걸려 있어,
 * 애플리케이션 로직이 뚫려도 DB 가 이중 결제를 막는 마지막 방어선이 된다.
 */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 128)
    private String idempotencyKey;

    @Column(name = "amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private PaymentStatus status;

    @Column(name = "pg_transaction_id", length = 64)
    private String pgTransactionId;

    @Column(name = "approved_at", nullable = false)
    private Instant approvedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Payment() {
        // JPA
    }

    private Payment(UUID orderId, String idempotencyKey, BigDecimal amount, PaymentStatus status,
                    String pgTransactionId, Instant approvedAt) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.idempotencyKey = idempotencyKey;
        this.amount = amount;
        this.status = status;
        this.pgTransactionId = pgTransactionId;
        this.approvedAt = approvedAt;
        this.createdAt = Instant.now();
    }

    public static Payment approved(UUID orderId, String idempotencyKey, BigDecimal amount, String pgTransactionId,
                                   Instant approvedAt) {
        return new Payment(orderId, idempotencyKey, amount, PaymentStatus.APPROVED, pgTransactionId, approvedAt);
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getPgTransactionId() {
        return pgTransactionId;
    }

    public Instant getApprovedAt() {
        return approvedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
