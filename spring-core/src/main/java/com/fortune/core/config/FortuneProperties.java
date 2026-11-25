package com.fortune.core.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code fortune.*} 설정 묶음. 값은 application.yml 에서 주입된다.
 *
 * <p>중첩 객체를 final 필드 + getter 로 노출하면 Spring Boot 의 JavaBean 바인딩이 내부 setter 로 값을 채운다.
 */
@ConfigurationProperties(prefix = "fortune")
public class FortuneProperties {

    private final Idempotency idempotency = new Idempotency();
    private final RateLimit rateLimit = new RateLimit();
    private final Internal internal = new Internal();
    private final Outbox outbox = new Outbox();
    private final Kafka kafka = new Kafka();
    private final Reading reading = new Reading();

    public Idempotency getIdempotency() {
        return idempotency;
    }

    public RateLimit getRateLimit() {
        return rateLimit;
    }

    public Internal getInternal() {
        return internal;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public Reading getReading() {
        return reading;
    }

    /** 결제 멱등성 정책. */
    public static class Idempotency {
        /** 처리 중 락 TTL. 프로세스가 죽어도 이 시간이 지나면 회수된다. */
        private Duration lockTtl = Duration.ofSeconds(10);
        /** 완료된 응답 보관 기간(Redis TTL + DB 만료 시각). */
        private Duration recordTtl = Duration.ofHours(24);
        private int keyMaxLength = 128;

        public Duration getLockTtl() {
            return lockTtl;
        }

        public void setLockTtl(Duration lockTtl) {
            this.lockTtl = lockTtl;
        }

        public Duration getRecordTtl() {
            return recordTtl;
        }

        public void setRecordTtl(Duration recordTtl) {
            this.recordTtl = recordTtl;
        }

        public int getKeyMaxLength() {
            return keyMaxLength;
        }

        public void setKeyMaxLength(int keyMaxLength) {
            this.keyMaxLength = keyMaxLength;
        }
    }

    /** 트래픽 급증 방어(슬라이딩 윈도우). */
    public static class RateLimit {
        private boolean enabled = true;
        private int requestsPerWindow = 100;
        private Duration window = Duration.ofSeconds(1);
        /** Redis 장애 시 요청을 통과시킬지 여부. true = 가용성 우선(권장: 결제 API 는 통과, 별도 경보). */
        private boolean failOpen = true;
        private List<String> pathPrefixes = List.of("/api/v1");

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getRequestsPerWindow() {
            return requestsPerWindow;
        }

        public void setRequestsPerWindow(int requestsPerWindow) {
            this.requestsPerWindow = requestsPerWindow;
        }

        public Duration getWindow() {
            return window;
        }

        public void setWindow(Duration window) {
            this.window = window;
        }

        public boolean isFailOpen() {
            return failOpen;
        }

        public void setFailOpen(boolean failOpen) {
            this.failOpen = failOpen;
        }

        public List<String> getPathPrefixes() {
            return pathPrefixes;
        }

        public void setPathPrefixes(List<String> pathPrefixes) {
            this.pathPrefixes = pathPrefixes;
        }
    }

    /** python-ai-worker 가 호출하는 내부 API 인증. */
    public static class Internal {
        private String token = "local-internal-token";

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }
    }

    /** 트랜잭션 아웃박스 릴레이. */
    public static class Outbox {
        private long relayIntervalMs = 500;
        private int batchSize = 50;
        private int maxAttempts = 5;
        private Duration baseBackoff = Duration.ofSeconds(5);

        public long getRelayIntervalMs() {
            return relayIntervalMs;
        }

        public void setRelayIntervalMs(long relayIntervalMs) {
            this.relayIntervalMs = relayIntervalMs;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getBaseBackoff() {
            return baseBackoff;
        }

        public void setBaseBackoff(Duration baseBackoff) {
            this.baseBackoff = baseBackoff;
        }
    }

    /** 토픽 이름/파티션 수. Python 워커의 config.yaml 과 반드시 일치해야 한다. */
    public static class Kafka {
        private String topic = "fortune.paid";
        private String dlqTopic = "fortune.paid.dlq";
        private int partitions = 3;
        private int replicas = 1;
        private Duration sendTimeout = Duration.ofSeconds(10);

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getDlqTopic() {
            return dlqTopic;
        }

        public void setDlqTopic(String dlqTopic) {
            this.dlqTopic = dlqTopic;
        }

        public int getPartitions() {
            return partitions;
        }

        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }

        public int getReplicas() {
            return replicas;
        }

        public void setReplicas(int replicas) {
            this.replicas = replicas;
        }

        public Duration getSendTimeout() {
            return sendTimeout;
        }

        public void setSendTimeout(Duration sendTimeout) {
            this.sendTimeout = sendTimeout;
        }
    }

    /** 조회 상태 캐시 TTL (python-ai-worker 와 동일 키/TTL 규약). */
    public static class Reading {
        private Duration statusTtl = Duration.ofDays(7);

        public Duration getStatusTtl() {
            return statusTtl;
        }

        public void setStatusTtl(Duration statusTtl) {
            this.statusTtl = statusTtl;
        }
    }
}
