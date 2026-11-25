package com.fortune.core.payment;

import com.fortune.core.common.ApiException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * 로컬/테스트용 목 PG.
 *
 * <p>금액이 0 이하이거나 {@code amount} 의 소수부가 990원(예: 4990)이면 승인 거절을 흉내 낸다
 * (실패 후 같은 Idempotency-Key 로 재시도하는 시나리오 검증용).
 */
@Component
@Profile("!prod")
public class MockPaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(MockPaymentGateway.class);

    @Override
    public PgResult charge(UUID orderId, BigDecimal amount, String idempotencyKey) {
        if (amount.signum() <= 0) {
            throw ApiException.unprocessable("PG_DECLINED", "결제 금액이 올바르지 않습니다.");
        }
        if (amount.remainder(BigDecimal.valueOf(100)).compareTo(BigDecimal.valueOf(90)) == 0) {
            throw ApiException.unprocessable("PG_DECLINED", "PG 승인이 거절되었습니다(모의 거절 규칙: 90원 단위).");
        }
        String transactionId = "mocktxn_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        log.debug("mock pg approved order={} amount={} key={} txn={}", orderId, amount, idempotencyKey, transactionId);
        return new PgResult(transactionId, Instant.now().plusMillis(ThreadLocalRandom.current().nextLong(0, 50)));
    }
}
