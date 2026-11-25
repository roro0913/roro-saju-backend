package com.fortune.core.idempotency;

/** 멱등성 판단 결과. */
public sealed interface IdempotencyDecision {

    /** 저장된 응답을 그대로 돌려준다(새 결제 없음). */
    record Replay(int status, String body) implements IdempotencyDecision {
    }

    /** 이 요청이 처리 주체다. 처리가 끝나면 {@code complete()}, 실패하면 {@code abort()} 를 호출해야 한다. */
    record Proceed(String key, String lockToken, String requestHash) implements IdempotencyDecision {
    }
}
