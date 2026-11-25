package com.fortune.core.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fortune.core.event.FortunePaidEvent;
import com.fortune.core.order.Order;
import com.fortune.core.payment.Payment;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * fortune.paid 메시지 계약 검증.
 * python-ai-worker 의 schemas.FortunePaidEvent 가 읽는 키 이름이 여기 고정된다.
 */
class FortunePaidEventTest {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    @DisplayName("이벤트 JSON 키가 파이썬 소비자 계약(카멜케이스)과 일치한다")
    void serialisesContractFields() throws Exception {
        Order order = Order.create("u-1001", "LOVE", new BigDecimal("4900"), LocalDate.of(1993, 9, 13),
                LocalTime.of(3, 53), "SOLAR", "M", "이직운?");
        Payment payment = Payment.approved(order.getId(), "idem-key-0001", new BigDecimal("4900"),
                "mocktxn_abc", Instant.parse("2026-11-24T01:00:00Z"));

        FortunePaidEvent event = FortunePaidEvent.of(order, payment, "evt-1", Instant.parse("2026-11-24T01:00:01Z"));

        JsonNode node = objectMapper.readTree(objectMapper.writeValueAsString(event));

        assertThat(node.get("eventId").asText()).isEqualTo("evt-1");
        assertThat(node.get("eventType").asText()).isEqualTo("fortune.paid");
        assertThat(node.get("orderId").asText()).isEqualTo(order.getId().toString());
        assertThat(node.get("paymentId").asText()).isEqualTo(payment.getId().toString());
        assertThat(node.get("amount").asInt()).isEqualTo(4900);
        assertThat(node.get("currency").asText()).isEqualTo("KRW");
        assertThat(node.get("fortuneType").asText()).isEqualTo("LOVE");
        assertThat(node.get("readingInput").get("birthDate").asText()).isEqualTo("1993-09-13");
        assertThat(node.get("readingInput").get("birthTime").asText()).isEqualTo("03:53");
        assertThat(node.get("readingInput").get("calendar").asText()).isEqualTo("SOLAR");
        assertThat(node.get("readingInput").get("gender").asText()).isEqualTo("M");
        assertThat(node.has("occurredAt")).isTrue();
    }

    @Test
    @DisplayName("생년월일 확인용: 주문에서 이벤트로 금액/사용자 정보가 그대로 전달된다")
    void carriesOrderFacts() {
        Order order = Order.create("u-2002", "MONEY", new BigDecimal("9900"), LocalDate.of(1990, 1, 1),
                null, "LUNAR", "F", null);
        Payment payment = Payment.approved(order.getId(), "idem-key-0002", new BigDecimal("9900"), "txn", Instant.now());

        FortunePaidEvent event = FortunePaidEvent.of(order, payment, UUID.randomUUID().toString(), Instant.now());

        assertThat(event.userId()).isEqualTo("u-2002");
        assertThat(event.readingInput().birthTime()).isNull();
        assertThat(event.readingInput().calendar()).isEqualTo("LUNAR");
        assertThat(event.readingInput().question()).isNull();
    }
}
