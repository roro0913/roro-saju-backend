package com.fortune.core.order;

/**
 * 주문 상태.
 *
 * <pre>
 * PENDING ──결제확정──▶ PAID ──AI생성완료──▶ COMPLETED
 *                          └──AI실패(3회)──▶ FAILED
 * </pre>
 */
public enum OrderStatus {
    PENDING,
    PAID,
    COMPLETED,
    FAILED,
    CANCELED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELED;
    }
}
