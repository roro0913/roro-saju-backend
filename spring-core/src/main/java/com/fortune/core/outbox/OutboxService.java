package com.fortune.core.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fortune.core.config.FortuneProperties;
import com.fortune.core.event.FortunePaidEvent;
import com.fortune.core.order.Order;
import com.fortune.core.payment.Payment;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 결제 트랜잭션 안에서 이벤트를 아웃박스에 적재한다.
 * 반드시 결제와 <b>같은 트랜잭션</b>(MANDATORY)이어야 "결제 성공 = 이벤트 적재" 가 보장된다.
 */
@Service
public class OutboxService {

    private final OutboxRepository repository;
    private final ObjectMapper objectMapper;
    private final FortuneProperties properties;

    public OutboxService(OutboxRepository repository, ObjectMapper objectMapper, FortuneProperties properties) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void appendFortunePaid(Order order, Payment payment) {
        FortunePaidEvent event = FortunePaidEvent.of(order, payment, UUID.randomUUID().toString(), Instant.now());
        repository.save(OutboxEvent.pending(
                order.getId().toString(),
                properties.getKafka().getTopic(),
                FortunePaidEvent.EVENT_TYPE,
                toJson(event)));
    }

    private String toJson(FortunePaidEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("fortune.paid 이벤트 직렬화 실패", e);
        }
    }
}
