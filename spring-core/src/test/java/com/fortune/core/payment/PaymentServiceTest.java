package com.fortune.core.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fortune.core.common.ApiException;
import com.fortune.core.idempotency.IdempotencyDecision;
import com.fortune.core.idempotency.IdempotencyService;
import com.fortune.core.order.Order;
import com.fortune.core.order.OrderRepository;
import com.fortune.core.order.OrderStatus;
import com.fortune.core.outbox.OutboxService;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PaymentServiceTest {

    private OrderRepository orderRepository;
    private PaymentRepository paymentRepository;
    private OutboxService outboxService;
    private PaymentGateway gateway;
    private IdempotencyService idempotencyService;
    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        orderRepository = mock(OrderRepository.class);
        paymentRepository = mock(PaymentRepository.class);
        outboxService = mock(OutboxService.class);
        gateway = mock(PaymentGateway.class);
        idempotencyService = mock(IdempotencyService.class);
        paymentService = new PaymentService(orderRepository, paymentRepository, outboxService, gateway,
                idempotencyService, new ObjectMapper().registerModule(new JavaTimeModule()));
    }

    private Order pendingOrder(BigDecimal amount) {
        return Order.create("u-1001", "TODAY", amount, LocalDate.of(1993, 9, 13), LocalTime.of(3, 53),
                "SOLAR", "M", null);
    }

    @Test
    @DisplayName("같은 Idempotency-Key 재요청은 저장된 응답을 replayed=true 로 돌려준다(새 결제 없음)")
    void replaysStoredResponse() {
        UUID orderId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        String stored = """
                {"orderId":"%s","paymentId":"%s","status":"PAID","amount":4900,"currency":"KRW",
                 "paidAt":"2026-11-24T01:00:00Z","replayed":false}
                """.formatted(orderId, paymentId);
        when(idempotencyService.begin(anyString(), anyString()))
                .thenReturn(new IdempotencyDecision.Replay(200, stored));

        PaymentService.ConfirmResult result = paymentService.confirm("idem-key-0001",
                new PaymentConfirmRequest(orderId, new BigDecimal("4900"), "KRW"));

        assertThat(result.status()).isEqualTo(200);
        assertThat(result.body().replayed()).isTrue();
        assertThat(result.body().paymentId()).isEqualTo(paymentId);

        verify(paymentRepository, never()).save(any());
        verify(gateway, never()).charge(any(), any(), anyString());
        verify(idempotencyService, never()).complete(any(), eq(200), any());
    }

    @Test
    @DisplayName("정상 결제는 결제/주문/아웃박스를 한 트랜잭션에 남기고 멱등 응답을 완료 처리한다")
    void confirmsPayment() {
        Order order = pendingOrder(new BigDecimal("4900"));
        when(idempotencyService.begin(anyString(), anyString()))
                .thenReturn(new IdempotencyDecision.Proceed("idem-key-0001", "lock-token", "hash"));
        when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
        when(paymentRepository.save(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(gateway.charge(any(), any(), anyString()))
                .thenReturn(new PaymentGateway.PgResult("mocktxn_1", Instant.parse("2026-11-24T01:00:00Z")));

        PaymentService.ConfirmResult result = paymentService.confirm("idem-key-0001",
                new PaymentConfirmRequest(order.getId(), new BigDecimal("4900"), "KRW"));

        assertThat(result.status()).isEqualTo(200);
        assertThat(result.body().replayed()).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PAID);

        ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository).save(paymentCaptor.capture());
        assertThat(paymentCaptor.getValue().getIdempotencyKey()).isEqualTo("idem-key-0001");
        verify(outboxService).appendFortunePaid(eq(order), any(Payment.class));
        verify(idempotencyService).complete(any(IdempotencyDecision.Proceed.class), eq(200), any());
    }

    @Test
    @DisplayName("금액이 다르면 422 AMOUNT_MISMATCH 로 막고 락을 반납한다")
    void rejectsAmountMismatch() {
        Order order = pendingOrder(new BigDecimal("4900"));
        when(idempotencyService.begin(anyString(), anyString()))
                .thenReturn(new IdempotencyDecision.Proceed("idem-key-0001", "lock-token", "hash"));
        when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

        PaymentConfirmRequest request = new PaymentConfirmRequest(order.getId(), new BigDecimal("1000"), "KRW");

        assertThatThrownBy(() -> paymentService.confirm("idem-key-0001", request))
                .isInstanceOf(ApiException.class)
                .satisfies(ex -> assertThat(((ApiException) ex).getCode()).isEqualTo("AMOUNT_MISMATCH"));

        verify(idempotencyService).abort(any(IdempotencyDecision.Proceed.class));
        verify(idempotencyService, never()).complete(any(), eq(200), any());
        verify(gateway, never()).charge(any(), any(), anyString());
    }

    @Test
    @DisplayName("이미 결제된 주문을 다른 키로 다시 결제하면 409 로 막는다")
    void rejectsAlreadyPaidOrder() {
        Order order = pendingOrder(new BigDecimal("4900"));
        order.markPaid();
        when(idempotencyService.begin(anyString(), anyString()))
                .thenReturn(new IdempotencyDecision.Proceed("idem-key-9999", "lock-token", "hash"));
        when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

        PaymentConfirmRequest request = new PaymentConfirmRequest(order.getId(), new BigDecimal("4900"), "KRW");

        assertThatThrownBy(() -> paymentService.confirm("idem-key-9999", request))
                .isInstanceOf(ApiException.class)
                .satisfies(ex -> assertThat(((ApiException) ex).getCode()).isEqualTo("ORDER_NOT_PAYABLE"));
        verify(gateway, never()).charge(any(), any(), anyString());
    }

    @Test
    @DisplayName("주문이 없으면 404 ORDER_NOT_FOUND")
    void rejectsUnknownOrder() {
        when(idempotencyService.begin(anyString(), anyString()))
                .thenReturn(new IdempotencyDecision.Proceed("idem-key-0001", "lock-token", "hash"));
        when(orderRepository.findById(any())).thenReturn(Optional.empty());

        PaymentConfirmRequest request = new PaymentConfirmRequest(UUID.randomUUID(), new BigDecimal("4900"), "KRW");

        assertThatThrownBy(() -> paymentService.confirm("idem-key-0001", request))
                .isInstanceOf(ApiException.class)
                .satisfies(ex -> assertThat(((ApiException) ex).getCode()).isEqualTo("ORDER_NOT_FOUND"));
    }
}
