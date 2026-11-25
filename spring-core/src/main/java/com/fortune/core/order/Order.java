package com.fortune.core.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "fortune_type", nullable = false, length = 32)
    private String fortuneType;

    @Column(name = "amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private OrderStatus status;

    @Column(name = "birth_date", nullable = false)
    private LocalDate birthDate;

    @Column(name = "birth_time")
    private LocalTime birthTime;

    @Column(name = "calendar_type", nullable = false, length = 8)
    private String calendarType;

    @Column(name = "gender", nullable = false, length = 1)
    private String gender;

    @Column(name = "question", length = 500)
    private String question;

    @Column(name = "failure_reason", length = 500)
    private String failureReason;

    /** 동시 결제 요청에 대한 낙관적 락. */
    @Version
    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {
        // JPA
    }

    private Order(String userId, String fortuneType, BigDecimal amount, LocalDate birthDate, LocalTime birthTime,
                  String calendarType, String gender, String question) {
        this.id = UUID.randomUUID();
        this.userId = userId;
        this.fortuneType = fortuneType;
        this.amount = amount;
        this.status = OrderStatus.PENDING;
        this.birthDate = birthDate;
        this.birthTime = birthTime;
        this.calendarType = calendarType;
        this.gender = gender;
        this.question = question;
        Instant now = Instant.now();
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static Order create(String userId, String fortuneType, BigDecimal amount, LocalDate birthDate,
                               LocalTime birthTime, String calendarType, String gender, String question) {
        return new Order(userId, fortuneType, amount, birthDate, birthTime, calendarType, gender, question);
    }

    // --- 상태 전이 -----------------------------------------------------
    public void markPaid() {
        requireStatus(OrderStatus.PENDING, "결제 확정");
        this.status = OrderStatus.PAID;
        touch();
    }

    public void markCompleted() {
        requireStatus(OrderStatus.PAID, "운세 생성 완료");
        this.status = OrderStatus.COMPLETED;
        this.failureReason = null;
        touch();
    }

    public void markFailed(String reason) {
        requireStatus(OrderStatus.PAID, "운세 생성 실패");
        this.status = OrderStatus.FAILED;
        this.failureReason = truncate(reason, 500);
        touch();
    }

    private void requireStatus(OrderStatus expected, String action) {
        if (status != expected) {
            throw new IllegalStateException(
                    "주문 %s 은(는) %s 상태에서 %s 할 수 없습니다.".formatted(id, status, action));
        }
    }

    private void touch() {
        this.updatedAt = Instant.now();
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    // --- getters ------------------------------------------------------
    public UUID getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public String getFortuneType() {
        return fortuneType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public LocalDate getBirthDate() {
        return birthDate;
    }

    public LocalTime getBirthTime() {
        return birthTime;
    }

    public String getCalendarType() {
        return calendarType;
    }

    public String getGender() {
        return gender;
    }

    public String getQuestion() {
        return question;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public long getVersion() {
        return version;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
