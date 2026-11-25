package com.fortune.core.order;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * 데모/부하테스트용 주문 생성 요청.
 * 실제 서비스에서는 상품 카탈로그에서 금액을 서버가 결정하고, 이 엔드포인트는 사라진다.
 */
public record CreateOrderRequest(
        @NotBlank @Size(max = 64) String userId,
        @NotBlank @Size(max = 32) String fortuneType,
        @NotNull @DecimalMin("0.0") @Digits(integer = 12, fraction = 2) BigDecimal amount,
        @NotNull LocalDate birthDate,
        LocalTime birthTime,
        @Pattern(regexp = "SOLAR|LUNAR") String calendarType,
        @Pattern(regexp = "M|F|U") String gender,
        @Size(max = 500) String question) {

    public CreateOrderRequest {
        if (calendarType == null || calendarType.isBlank()) {
            calendarType = "SOLAR";
        }
        if (gender == null || gender.isBlank()) {
            gender = "U";
        }
    }
}
