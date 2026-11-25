package com.fortune.core.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID orderId,
        String userId,
        String fortuneType,
        BigDecimal amount,
        OrderStatus status,
        UUID paymentId,
        String failureReason,
        Instant createdAt,
        Instant updatedAt) {

    public static OrderResponse of(Order order, UUID paymentId) {
        return new OrderResponse(
                order.getId(),
                order.getUserId(),
                order.getFortuneType(),
                order.getAmount(),
                order.getStatus(),
                paymentId,
                order.getFailureReason(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
