package com.fortune.core.common;

import java.time.Instant;

/** 모든 오류 응답의 공통 형태. */
public record ErrorResponse(String code, String message, int status, String path, Instant timestamp) {

    public static ErrorResponse of(String code, String message, int status, String path) {
        return new ErrorResponse(code, message, status, path, Instant.now());
    }
}
