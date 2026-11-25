package com.fortune.core.internal;

import com.fortune.core.order.OrderStatus;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * AI 워커가 주문 상태를 바꿀 때 보내는 본문.
 * 워커는 결과를 만들었으니 COMPLETED, 재시도를 모두 소진했으면 FAILED 만 보낸다.
 */
public record OrderStatusUpdateRequest(
        @NotNull OrderStatus status,
        @Size(max = 500) String reason,
        Integer attempts) {
}
