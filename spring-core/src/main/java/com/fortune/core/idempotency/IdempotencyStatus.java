package com.fortune.core.idempotency;

public enum IdempotencyStatus {
    /** 처리 중(진행 중) 상태. 이 상태로 응답하려는 재요청은 409. */
    IN_PROGRESS,
    /** 처리 완료. 저장된 응답을 재생한다. */
    COMPLETED
}
