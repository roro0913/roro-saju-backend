package com.fortune.core.payment;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * 결제 확정.
     *
     * <p>{@code Idempotency-Key} 는 필수다. 같은 키의 재요청은 200 + 저장된 응답(재결제 없음),
     * 처리 중인 동시 요청은 409, 같은 키에 다른 본문이면 422 를 돌려준다.
     */
    @PostMapping("/confirm")
    public ResponseEntity<PaymentConfirmResponse> confirm(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody PaymentConfirmRequest request) {
        PaymentService.ConfirmResult result = paymentService.confirm(idempotencyKey, request);
        return ResponseEntity.status(result.status()).body(result.body());
    }
}
