package com.fortune.core.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/** 만료된 멱등성 기록 정리. (수십만 건이 쌓이면 PK 인덱스만으로도 비용이 되므로 매일 훑는다.) */
@Component
public class IdempotencyCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCleanupJob.class);

    private final IdempotencyRepository repository;

    public IdempotencyCleanupJob(IdempotencyRepository repository) {
        this.repository = repository;
    }

    @Scheduled(cron = "${fortune.idempotency.cleanup-cron:0 17 4 * * *}")
    @Transactional
    public void purgeExpired() {
        int deleted = repository.deleteExpired(Instant.now());
        if (deleted > 0) {
            log.info("purged {} expired idempotency records", deleted);
        }
    }
}
