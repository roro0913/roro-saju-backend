package com.fortune.core.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * PG(결제대행사) 연동 지점.
 *
 * <p>실제 구현에서는 토스페이먼츠/카카오페이 등의 승인 API 를 호출하고 타임아웃/부분성공을 다룬다.
 * 여기서는 로컬 검증용 목 구현만 둔다. 교체 지점을 한 곳으로 모아두는 것이 목적이다.
 */
public interface PaymentGateway {

    PgResult charge(UUID orderId, BigDecimal amount, String idempotencyKey);

    record PgResult(String transactionId, Instant approvedAt) {
    }
}
