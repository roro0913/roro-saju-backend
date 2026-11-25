package com.fortune.core.payment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 결제 확정 응답. Redis/DB 에 직렬화해 두었다가 중복 요청 시 **그대로 재생**한다.
 *
 * @param replayed true 면 저장된 응답을 재생한 것(새 결제 아님)
 */
public record PaymentConfirmResponse(
        UUID orderId,
        UUID paymentId,
        String status,
        BigDecimal amount,
        String currency,
        Instant paidAt,
        boolean replayed) {

    public static PaymentConfirmResponse of(UUID orderId, UUID paymentId, BigDecimal amount, String currency,
                                            Instant paidAt) {
        return new PaymentConfirmResponse(orderId, paymentId, "PAID", amount, currency, paidAt, false);
    }

    public PaymentConfirmResponse asReplayed() {
        return new PaymentConfirmResponse(orderId, paymentId, status, amount, currency, paidAt, true);
    }
}
