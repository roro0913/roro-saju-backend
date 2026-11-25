package com.fortune.core.payment;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * 결제 확정 요청.
 *
 * <p>지문(fingerprint)은 "같은 Idempotency-Key 인데 본문이 다른 요청"을 걸러내는 데 쓴다.
 * 금액 표기 차이(4900 vs 4900.00)로 오탐이 나지 않도록 정규화한다.
 */
public record PaymentConfirmRequest(
        @NotNull UUID orderId,
        @NotNull @DecimalMin(value = "0.0", inclusive = false) @Digits(integer = 12, fraction = 2) BigDecimal amount,
        @Size(min = 3, max = 3) String currency) {

    public PaymentConfirmRequest {
        if (currency == null || currency.isBlank()) {
            currency = "KRW";
        }
    }

    public String fingerprintSource() {
        return orderId + "|" + amount.stripTrailingZeros().toPlainString() + "|" + currency;
    }
}
