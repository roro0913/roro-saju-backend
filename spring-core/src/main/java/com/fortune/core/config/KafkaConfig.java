package com.fortune.core.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Kafka 토픽 정의 + 프로듀서 설정 검증.
 *
 * <p>프로듀서 자체는 Spring Boot 자동 구성({@code spring.kafka.producer.*})을 그대로 쓴다.
 * 여기서는 (1) 토픽을 선언해 기동 시 브로커에 만들고, (2) acks/멱등성이 설정대로 들어갔는지 기동 시 확인한다.
 */
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final FortuneProperties properties;

    public KafkaConfig(FortuneProperties properties) {
        this.properties = properties;
    }

    @Bean
    public NewTopic fortunePaidTopic() {
        return TopicBuilder.name(properties.getKafka().getTopic())
                .partitions(properties.getKafka().getPartitions())
                .replicas(properties.getKafka().getReplicas())
                .build();
    }

    @Bean
    public NewTopic fortunePaidDlqTopic() {
        return TopicBuilder.name(properties.getKafka().getDlqTopic())
                .partitions(1)
                .replicas(properties.getKafka().getReplicas())
                .build();
    }

    /**
     * 설정 오타(예: acks=1, 멱등성 off)는 유실/중복으로 이어지므로 기동 시 바로 경보를 남긴다.
     * 여기서 예외를 던져 기동을 막지는 않는다(로컬 개발 편의). 운영 프로파일에서는 fail-fast 로 바꿔도 좋다.
     */
    @Bean
    public ApplicationRunner kafkaProducerSettingsCheck(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> {
            ProducerFactory<String, String> factory = kafkaTemplate.getProducerFactory();
            Object acks = factory.getConfigurationProperties().get(ProducerConfig.ACKS_CONFIG);
            Object idempotence = factory.getConfigurationProperties().get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
            boolean acksAll = "all".equals(String.valueOf(acks)) || "-1".equals(String.valueOf(acks));
            boolean idempotent = Boolean.parseBoolean(String.valueOf(idempotence));
            if (acksAll && idempotent) {
                log.info("kafka producer OK: acks=all, enable.idempotence=true, topic={}", properties.getKafka().getTopic());
            } else {
                log.error("kafka producer misconfigured: acks={}, enable.idempotence={} (유실/중복 위험)", acks, idempotence);
            }
        };
    }
}
