package com.fortune.core.ratelimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fortune.core.common.ErrorResponse;
import com.fortune.core.config.FortuneProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * API 경로에 대한 레이트 리밋 필터.
 *
 * <p>식별자는 {@code X-User-Id} → {@code X-Forwarded-For} 첫 IP → 원격 주소 순으로 정한다.
 * (로그인 사용자 기준 제한이 기본, 비로그인/부하테스트는 IP 기준)
 *
 * <p>Redis 장애 시 정책은 설정으로 결정한다({@code fortune.rate-limit.fail-open}).
 * 기본값 true = 결제 API 가용성 우선. 대신 경보를 남겨 운영에서 바로 인지하게 한다.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RateLimitFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final RedisSlidingWindowRateLimiter limiter;
    private final FortuneProperties properties;
    private final ObjectMapper objectMapper;

    public RateLimitFilter(RedisSlidingWindowRateLimiter limiter, FortuneProperties properties,
                           ObjectMapper objectMapper) {
        this.limiter = limiter;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        FortuneProperties.RateLimit config = properties.getRateLimit();
        if (!config.isEnabled()) {
            return true;
        }
        if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
            return true;
        }
        String path = request.getRequestURI();
        List<String> prefixes = config.getPathPrefixes();
        return prefixes.stream().noneMatch(path::startsWith);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String clientKey = resolveClientKey(request);
        try {
            RedisSlidingWindowRateLimiter.RateLimitDecision decision = limiter.tryAcquire("api", clientKey);
            response.setHeader("X-RateLimit-Limit", String.valueOf(decision.limit()));
            response.setHeader("X-RateLimit-Remaining", String.valueOf(Math.max(0, decision.remaining())));
            if (!decision.allowed()) {
                long retryAfterSeconds = Math.max(1, (decision.retryAfterMillis() + 999) / 1000);
                response.setStatus(429);
                response.setHeader("Retry-After", String.valueOf(retryAfterSeconds));
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                response.setCharacterEncoding(StandardCharsets.UTF_8.name());
                ErrorResponse body = ErrorResponse.of("RATE_LIMITED",
                        "요청이 너무 많습니다. " + retryAfterSeconds + "초 후 다시 시도해 주세요.",
                        429, request.getRequestURI());
                response.getWriter().write(objectMapper.writeValueAsString(body));
                return;
            }
        } catch (RuntimeException ex) {
            if (properties.getRateLimit().isFailOpen()) {
                // Redis 하나 죽었다고 결제를 막지는 않는다. 대신 흔적을 남긴다.
                log.warn("rate limiter unavailable, failing open (client={}): {}", clientKey, ex.getMessage());
            } else {
                log.error("rate limiter unavailable, failing closed (client={})", clientKey, ex);
                response.setStatus(503);
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                response.setCharacterEncoding(StandardCharsets.UTF_8.name());
                response.getWriter().write(objectMapper.writeValueAsString(ErrorResponse.of(
                        "RATE_LIMITER_UNAVAILABLE", "잠시 후 다시 시도해 주세요.", 503, request.getRequestURI())));
                return;
            }
        }
        chain.doFilter(request, response);
    }

    private String resolveClientKey(HttpServletRequest request) {
        String userId = request.getHeader("X-User-Id");
        if (userId != null && !userId.isBlank()) {
            return "u:" + sanitise(userId);
        }
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return "ip:" + sanitise(forwarded.split(",")[0].trim());
        }
        return "ip:" + sanitise(request.getRemoteAddr());
    }

    private String sanitise(String value) {
        return value.replaceAll("[^A-Za-z0-9._:-]", "_");
    }
}
