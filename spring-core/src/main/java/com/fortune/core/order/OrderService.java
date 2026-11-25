package com.fortune.core.order;

import com.fortune.core.common.ApiException;
import com.fortune.core.common.RedisKeys;
import com.fortune.core.config.FortuneProperties;
import com.fortune.core.payment.Payment;
import com.fortune.core.payment.PaymentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final PaymentRepository paymentRepository;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final FortuneProperties properties;

    public OrderService(OrderRepository orderRepository, PaymentRepository paymentRepository,
                        StringRedisTemplate redis, ObjectMapper objectMapper, FortuneProperties properties) {
        this.orderRepository = orderRepository;
        this.paymentRepository = paymentRepository;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        Order order = Order.create(
                request.userId(),
                request.fortuneType(),
                request.amount(),
                request.birthDate(),
                request.birthTime(),
                request.calendarType(),
                request.gender(),
                request.question());
        orderRepository.save(order);
        runAfterCommit(() -> writeReadingStatus(order.getId(), "PENDING", 0, "결제 대기"));
        return OrderResponse.of(order, null);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID orderId) {
        Order order = load(orderId);
        UUID paymentId = paymentRepository.findByOrderId(orderId).map(Payment::getId).orElse(null);
        return OrderResponse.of(order, paymentId);
    }

    /**
     * python-ai-worker 가 호출하는 상태 반영. 같은 상태면 아무 것도 하지 않는다(멱등).
     */
    @Transactional
    public OrderResponse updateFromWorker(UUID orderId, OrderStatus target, String reason) {
        Order order = load(orderId);
        if (order.getStatus() == target) {
            return OrderResponse.of(order, null);
        }
        switch (target) {
            case COMPLETED -> order.markCompleted();
            case FAILED -> order.markFailed(reason);
            default -> throw ApiException.badRequest("UNSUPPORTED_STATUS_TRANSITION",
                    "AI 워커가 요청할 수 있는 상태는 COMPLETED, FAILED 뿐입니다: " + target);
        }
        orderRepository.save(order);
        log.info("order {} -> {} (reason={})", orderId, target, reason);
        return OrderResponse.of(order, null);
    }

    private Order load(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> ApiException.notFound("ORDER_NOT_FOUND", "주문을 찾을 수 없습니다: " + orderId));
    }

    /**
     * 조회 API(python-ai-worker)가 읽는 상태 캐시를 미리 심어 둔다.
     * 키/JSON 규약은 python 의 reading:status:{orderId} 와 동일해야 한다.
     */
    private void writeReadingStatus(UUID orderId, String status, int attempts, String message) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("orderId", orderId.toString());
            payload.put("status", status);
            payload.put("attempts", attempts);
            payload.put("message", message);
            payload.put("updatedAt", Instant.now().toString());
            String json = objectMapper.writeValueAsString(payload);
            redis.opsForValue().set(RedisKeys.readingStatus(orderId), json, properties.getReading().getStatusTtl());
        } catch (JsonProcessingException | RuntimeException ex) {
            // 상태 캐시는 보조 정보다. 실패해도 주문 생성은 성공시킨다.
            log.warn("failed to seed reading status for order {}: {}", orderId, ex.getMessage());
        }
    }

    private void runAfterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }
}
