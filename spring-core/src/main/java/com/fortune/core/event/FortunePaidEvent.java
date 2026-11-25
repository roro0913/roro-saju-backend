package com.fortune.core.event;

import com.fortune.core.order.Order;
import com.fortune.core.payment.Payment;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * {@code fortune.paid} 토픽 메시지 계약 (key = orderId).
 *
 * <p>python-ai-worker/app/schemas.py 의 {@code FortunePaidEvent} 와 필드명이 1:1 로 맞아야 한다.
 * 필드를 바꿀 때는 양쪽을 함께 바꾸고, 소비자가 모르는 필드는 무시하도록(extra=ignore) 두어
 * <b>필드 추가는 하위 호환이 되게</b> 한다.
 */
public record FortunePaidEvent(
        String eventId,
        String eventType,
        Instant occurredAt,
        String orderId,
        String paymentId,
        String userId,
        BigDecimal amount,
        String currency,
        String fortuneType,
        ReadingInput readingInput) {

    public static final String EVENT_TYPE = "fortune.paid";

    public record ReadingInput(
            String birthDate,
            String birthTime,
            String calendar,
            String gender,
            String question) {
    }

    public static FortunePaidEvent of(Order order, Payment payment, String eventId, Instant occurredAt) {
        DateTimeFormatter hhmm = DateTimeFormatter.ofPattern("HH:mm");
        ReadingInput input = new ReadingInput(
                order.getBirthDate() == null ? null : order.getBirthDate().toString(),
                order.getBirthTime() == null ? null : order.getBirthTime().format(hhmm),
                order.getCalendarType(),
                order.getGender(),
                order.getQuestion());
        return new FortunePaidEvent(
                eventId,
                EVENT_TYPE,
                occurredAt,
                order.getId().toString(),
                payment.getId().toString(),
                order.getUserId(),
                order.getAmount(),
                "KRW",
                order.getFortuneType(),
                input);
    }
}
