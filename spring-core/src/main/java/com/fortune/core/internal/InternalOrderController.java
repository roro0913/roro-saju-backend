package com.fortune.core.internal;

import com.fortune.core.common.ApiException;
import com.fortune.core.config.FortuneProperties;
import com.fortune.core.order.OrderResponse;
import com.fortune.core.order.OrderService;
import jakarta.validation.Valid;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * AI 워커 전용 내부 API. 외부에 노출하지 않는다(게이트웨이/인그레스에서 /internal/** 차단).
 *
 * <p>공유 DB 를 쓰지 않고 이 API 로만 상태를 바꾸므로, 스키마 변경이 워커를 깨뜨리지 않는다.
 * 인증은 정적 토큰(예제) — 운영에서는 mTLS 또는 사내 서비스 토큰 발급으로 교체한다.
 */
@RestController
@RequestMapping("/internal/v1/orders")
public class InternalOrderController {

    private static final Logger log = LoggerFactory.getLogger(InternalOrderController.class);

    private final OrderService orderService;
    private final FortuneProperties properties;

    public InternalOrderController(OrderService orderService, FortuneProperties properties) {
        this.orderService = orderService;
        this.properties = properties;
    }

    @PatchMapping("/{orderId}/status")
    public ResponseEntity<OrderResponse> updateStatus(
            @PathVariable UUID orderId,
            @RequestHeader(value = "X-Internal-Token", required = false) String token,
            @Valid @RequestBody OrderStatusUpdateRequest request) {
        verifyToken(token);
        OrderResponse response = orderService.updateFromWorker(orderId, request.status(), request.reason());
        log.debug("order {} status -> {} by ai worker (attempts={})", orderId, request.status(), request.attempts());
        return ResponseEntity.ok(response);
    }

    private void verifyToken(String token) {
        String expected = properties.getInternal().getToken();
        if (token == null || !MessageDigest.isEqual(
                token.getBytes(StandardCharsets.UTF_8), expected.getBytes(StandardCharsets.UTF_8))) {
            throw ApiException.forbidden("INTERNAL_TOKEN_INVALID", "내부 API 토큰이 올바르지 않습니다.");
        }
    }
}
