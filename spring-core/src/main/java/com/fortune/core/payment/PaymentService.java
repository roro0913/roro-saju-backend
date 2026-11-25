package com.fortune.core.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fortune.core.common.ApiException;
import com.fortune.core.common.RequestHasher;
import com.fortune.core.idempotency.IdempotencyDecision;
import com.fortune.core.idempotency.IdempotencyService;
import com.fortune.core.order.Order;
import com.fortune.core.order.OrderRepository;
import com.fortune.core.order.OrderStatus;
import com.fortune.core.outbox.OutboxService;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 결제 확정 유스케이스.
 *
 * <p>흐름: 멱등성 판단 → 주문/금액 검증 → PG 승인 → 결제 기록 → 주문 PAID → 아웃박스 적재(+ 멱등 응답 저장)
 * 모두 <b>하나의 로컬 트랜잭션</b>이다. Kafka 발행은 이 트랜잭션 밖(릴레이)에서 일어난다.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final OrderRepository orderRepository;
    private final PaymentRepository paymentRepository;
    private final OutboxService outboxService;
    private final PaymentGateway paymentGateway;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    public PaymentService(OrderRepository orderRepository, PaymentRepository paymentRepository,
                          OutboxService outboxService, PaymentGateway paymentGateway,
                          IdempotencyService idempotencyService, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.paymentRepository = paymentRepository;
        this.outboxService = outboxService;
        this.paymentGateway = paymentGateway;
        this.idempotencyService = idempotencyService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ConfirmResult confirm(String idempotencyKey, PaymentConfirmRequest request) {
        String fingerprint = RequestHasher.sha256Hex(request.fingerprintSource());

        IdempotencyDecision decision = idempotencyService.begin(idempotencyKey, fingerprint);
        if (decision instanceof IdempotencyDecision.Replay replay) {
            // 이미 처리된 요청: 새 결제 없이 저장된 응답을 그대로 돌려준다(새로고침/중복 클릭에 안전).
            return new ConfirmResult(replay.status(), deserialise(replay.body()));
        }

        IdempotencyDecision.Proceed handle = (IdempotencyDecision.Proceed) decision;
        try {
            PaymentConfirmResponse response = doConfirm(handle, request);
            idempotencyService.complete(handle, 200, response);
            return new ConfirmResult(200, response);
        } catch (RuntimeException ex) {
            // 롤백되는 트랜잭션이므로 멱등 기록도 사라진다 → 같은 키로 재시도 가능. 락만 즉시 반납.
            idempotencyService.abort(handle);
            throw ex;
        }
    }

    private PaymentConfirmResponse doConfirm(IdempotencyDecision.Proceed handle, PaymentConfirmRequest request) {
        Order order = orderRepository.findById(request.orderId())
                .orElseThrow(() -> ApiException.notFound("ORDER_NOT_FOUND", "주문을 찾을 수 없습니다: " + request.orderId()));

        if (order.getStatus() != OrderStatus.PENDING) {
            // 다른 멱등 키로 이미 결제된 주문을 또 결제하려는 시도 = 이중 결제 시도.
            throw ApiException.conflict("ORDER_NOT_PAYABLE",
                    "주문 상태가 " + order.getStatus() + " 이므로 결제할 수 없습니다.");
        }
        if (order.getAmount().compareTo(request.amount()) != 0) {
            throw ApiException.unprocessable("AMOUNT_MISMATCH",
                    "요청 금액(%s)과 주문 금액(%s)이 다릅니다.".formatted(request.amount(), order.getAmount()));
        }

        PaymentGateway.PgResult pg = paymentGateway.charge(order.getId(), order.getAmount(), handle.key());

        Payment payment = Payment.approved(order.getId(), handle.key(), order.getAmount(), pg.transactionId(),
                pg.approvedAt());
        paymentRepository.save(payment);

        order.markPaid();
        orderRepository.save(order);

        outboxService.appendFortunePaid(order, payment);

        log.info("payment confirmed order={} payment={} amount={} key={}",
                order.getId(), payment.getId(), order.getAmount(), handle.key());

        return PaymentConfirmResponse.of(order.getId(), payment.getId(), order.getAmount(), "KRW", pg.approvedAt());
    }

    private PaymentConfirmResponse deserialise(String body) {
        try {
            return objectMapper.readValue(body, PaymentConfirmResponse.class).asReplayed();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("저장된 멱등 응답을 읽을 수 없습니다.", e);
        }
    }

    /** 컨트롤러가 상태코드와 본문을 그대로 돌려주기 위한 결과. */
    public record ConfirmResult(int status, PaymentConfirmResponse body) {
    }

    /** 결제 단건 조회(운영/디버깅용). */
    @Transactional(readOnly = true)
    public Optional<Payment> findByOrderId(java.util.UUID orderId) {
        return paymentRepository.findByOrderId(orderId);
    }
}
