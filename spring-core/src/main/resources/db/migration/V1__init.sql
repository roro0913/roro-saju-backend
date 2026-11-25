-- 사주 서비스 코어 스키마 (PostgreSQL 16)
-- 금액/상태/멱등성 제약이 이 파일에 모여 있다. 여기 있는 UNIQUE 들이 애플리케이션 버그의 마지막 방어선이다.

CREATE TABLE orders (
    id            UUID PRIMARY KEY,
    user_id       VARCHAR(64)    NOT NULL,
    fortune_type  VARCHAR(32)    NOT NULL,
    amount        NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    status        VARCHAR(16)    NOT NULL CHECK (status IN ('PENDING', 'PAID', 'COMPLETED', 'FAILED', 'CANCELED')),
    birth_date    DATE           NOT NULL,
    birth_time    TIME,
    calendar_type VARCHAR(8)     NOT NULL DEFAULT 'SOLAR' CHECK (calendar_type IN ('SOLAR', 'LUNAR')),
    gender        VARCHAR(1)     NOT NULL DEFAULT 'U' CHECK (gender IN ('M', 'F', 'U')),
    question      VARCHAR(500),
    failure_reason VARCHAR(500),
    version       BIGINT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_user_created ON orders (user_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders (status);

CREATE TABLE payments (
    id                UUID PRIMARY KEY,
    order_id          UUID           NOT NULL REFERENCES orders (id),
    idempotency_key   VARCHAR(128)   NOT NULL,
    amount            NUMERIC(12, 2) NOT NULL,
    status            VARCHAR(16)    NOT NULL CHECK (status IN ('APPROVED', 'FAILED')),
    pg_transaction_id VARCHAR(64),
    approved_at       TIMESTAMPTZ    NOT NULL,
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT now(),
    -- 1 주문 = 1 결제: 같은 주문에 두 번 승인되지 않는다.
    CONSTRAINT uk_payments_order UNIQUE (order_id),
    -- 같은 멱등 키로 두 번 승인되지 않는다.
    CONSTRAINT uk_payments_idempotency_key UNIQUE (idempotency_key)
);

CREATE INDEX idx_payments_created ON payments (created_at DESC);

CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    request_hash    VARCHAR(64)  NOT NULL,
    status          VARCHAR(16)  NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED')),
    response_status INTEGER,
    response_body   TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ  NOT NULL
);

CREATE INDEX idx_idempotency_expires ON idempotency_keys (expires_at);

CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,
    aggregate_id    VARCHAR(64)  NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         TEXT         NOT NULL,
    status          VARCHAR(16)  NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    attempts        INTEGER      NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_error      VARCHAR(500),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

-- 릴레이가 훑는 인덱스: (status, next_attempt_at)
CREATE INDEX idx_outbox_due ON outbox_events (status, next_attempt_at);
CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_id);
