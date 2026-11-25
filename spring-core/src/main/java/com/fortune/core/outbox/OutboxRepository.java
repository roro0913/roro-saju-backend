package com.fortune.core.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query("""
            select e from OutboxEvent e
            where e.status = :status and e.nextAttemptAt <= :now
            order by e.createdAt asc
            """)
    List<OutboxEvent> findDue(@Param("status") OutboxStatus status, @Param("now") Instant now, Pageable pageable);

    long countByStatus(OutboxStatus status);
}
