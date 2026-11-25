package com.fortune.core.outbox;

import com.fortune.core.config.FortuneProperties;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 아웃박스 → Kafka 릴레이.
 *
 * <p>결제 API 스레드는 Kafka 를 직접 만지지 않는다. 이 릴레이가 비동기로 밀어내므로
 * Kafka 지연/장애가 결제 응답 시간에 영향을 주지 않는다. 발행에 성공한 행만 PUBLISHED 로 바꾼다.
 *
 * <p>여러 인스턴스가 동시에 돌면 같은 이벤트가 중복 발행될 수 있다(최소 1회 전달).
 * 소비자는 eventId 로 중복을 걸러내므로(at-least-once + 멱등 소비) 결과는 한 번만 반영된다.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final FortuneProperties properties;

    public OutboxRelay(OutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                       FortuneProperties properties) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${fortune.outbox.relay-interval-ms:500}")
    public void relay() {
        List<OutboxEvent> due;
        try {
            due = repository.findDue(
                    OutboxStatus.PENDING,
                    Instant.now(),
                    PageRequest.of(0, properties.getOutbox().getBatchSize()));
        } catch (RuntimeException ex) {
            log.warn("outbox poll failed: {}", ex.getMessage());
            return;
        }
        for (OutboxEvent event : due) {
            publish(event);
        }
    }

    private void publish(OutboxEvent event) {
        try {
            kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                    .get(properties.getKafka().getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            event.markPublished();
            repository.save(event);
            log.info("outbox published id={} topic={} order={}", event.getId(), event.getTopic(), event.getAggregateId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            event.markAttemptFailed(rootMessage(e), properties.getOutbox().getMaxAttempts(),
                    properties.getOutbox().getBaseBackoff());
            repository.save(event);
            log.warn("outbox publish failed id={} attempts={} status={}: {}",
                    event.getId(), event.getAttempts(), event.getStatus(), rootMessage(e));
        }
    }

    private String rootMessage(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
