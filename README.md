# 🏗️ 사주 서비스 MSA 백엔드 (Spring + Python + Kafka + Redis)

트래픽 급증 대응 · 모듈 간 장애 격리 · 결제 멱등성(중복 결제 방지) · Kafka/Redis 연동을 한 번에 보여주는
**실행 가능한** 백엔드 초안입니다.

- 결제/주문은 **Java 21 · Spring Boot 3.3** (`spring-core`)
- 운세 생성(AI)은 **Python 3.12 · FastAPI · aiokafka** (`python-ai-worker`)
- 두 서비스는 **Kafka 토픽 `fortune.paid`** 로만 이어지고, 서로의 DB 를 직접 만지지 않습니다.
- 인프라는 `docker-compose.yml` 하나로 PostgreSQL 16 · Redis 7 · Kafka 3.9(KRaft) · 두 서비스가 모두 뜹니다.

---

## 1. 한눈에 보는 구조

```
                        ┌──────────────────────────────┐
   사용자 / 앱  ───────▶│  spring-core  (:8080)         │
   (HTTP)               │  ┌────────────────────────┐   │
                        │  │ RateLimitFilter        │   │  ① 슬라이딩 윈도우(Redis Lua)
                        │  │  └ Redis ZSET          │   │     → 과도한 요청 429
                        │  ├────────────────────────┤   │
                        │  │ PaymentService         │   │  ② Idempotency-Key
                        │  │  ├ Redis 락/응답캐시   │   │     SET NX EX + 결과 재생
                        │  │  └ idempotency_keys DB │   │     + UNIQUE 제약(이중 방어)
                        │  ├────────────────────────┤   │
                        │  │ Order / Payment (JPA)  │───┼──▶ PostgreSQL 16 (:5432)
                        │  ├────────────────────────┤   │
                        │  │ OutboxService          │   │  ③ 결제와 같은 트랜잭션으로 적재
                        │  └────────────────────────┘   │
                        │  OutboxRelay (@Scheduled)     │  ④ 비동기로 Kafka 발행
                        └───────────────┬───────────────┘     (acks=all, 멱등 프로듀서)
                                        │
                          topic: fortune.paid  (key = orderId, 3 partitions)
                                        │
                        ┌───────────────▼───────────────┐
                        │  python-ai-worker  (:8000)     │
                        │  ┌────────────────────────┐    │  ⑤ 수동 커밋 + Semaphore
                        │  │ FortuneConsumer        │    │     + 지수 백오프 재시도
                        │  │  └ OffsetTracker       │    │
                        │  ├────────────────────────┤    │
                        │  │ ReadingHandler         │───┼──▶ Redis (:6379)
                        │  │  ├ LLM 캐시(동일 입력) │    │     reading:status / reading:result
                        │  │  └ 상태 PENDING→…      │    │     reading:cache / reading:dedupe
                        │  ├────────────────────────┤    │
                        │  │ FastAPI 조회 API       │    │  GET /api/v1/readings/{orderId}
                        │  └────────────────────────┘    │
                        │  실패 3회 → DLQ 발행 + FAILED  │──▶ topic: fortune.paid.dlq
                        └───────────────┬────────────────┘
                                        │ 내부 API(PATCH, X-Internal-Token)
                                        ▼
                              spring-core /internal/v1/orders/{id}/status
```

### 상태 흐름

```
주문  PENDING ──결제확정──▶ PAID ──AI완료──▶ COMPLETED
                              └──3회실패──▶ FAILED

읽기  PENDING ──▶ PROCESSING ──▶ COMPLETED
                        └──────▶ FAILED   (DLQ 로 원본 보존)
```

---

## 2. 설계 의도 (왜 이렇게 했나)

### ① 결제 멱등성 — "새로고침해도 두 번 결제되지 않는다"

두 겹으로 막습니다. 하나는 속도/동시성용, 하나는 최종 진실용입니다.

| 단계 | 수단 | 하는 일 |
|---|---|---|
| 1 | **Redis 락** `SET idem:lock:{key} {token} NX EX 10s` | 같은 키의 동시 요청 중 **한 건만** 처리 주체가 된다 |
| 2 | **Redis 응답 캐시** `idem:resp:{key}` (hash) | 끝난 요청의 응답을 그대로 재생 → PG 호출 없음 |
| 3 | **PostgreSQL** `idempotency_keys` PK(=key) + `payments.idempotency_key` UNIQUE | Redis 가 날아가도 DB 가 이중 결제를 거부 |

- **같은 키 재요청** → `200` + 저장된 응답(`replayed: true`). 새 결제 없음.
- **처리 중 동시 요청** → `409 IDEMPOTENCY_IN_PROGRESS` + `Retry-After: 1`.
- **같은 키, 다른 본문** → `422 IDEMPOTENCY_KEY_REUSED` (키 재사용 사고 방지).
- **다른 키, 이미 결제된 주문** → `409 ORDER_NOT_PAYABLE` (우회 이중 결제 차단).
- **처리 실패** → 트랜잭션이 롤백되므로 멱등 기록도 사라지고 락도 즉시 반납 → **같은 키로 재시도 가능**.

세 가지 세부 판단:

1. **락 해제는 compare-and-delete** — 내 토큰일 때만 지웁니다. 처리 시간이 길어져 락 TTL(10초)이 먼저 만료된 뒤,
   다른 요청이 새로 잡은 락을 내가 지워버리는 사고를 막습니다.
2. **Redis 쓰기는 커밋 이후(afterCommit)** — DB 가 롤백됐는데 캐시에 "완료 응답"이 남는 불일치를 막습니다.
   캐시 기록이 실패해도 DB 기록이 남아 다음 요청은 DB 에서 재생됩니다.
3. **응답 지문(fingerprint)** — `orderId|amount|currency` 를 SHA-256 으로 정규화해 비교합니다(4900 과 4900.00 을 같은 요청으로).

### ② 트래픽 급증 대응

- **슬라이딩 윈도우 레이트 리미터**(Redis ZSET + Lua): 고정 윈도우 카운터는 경계에서 2배 버스트가 생기므로,
  요청 시각을 ZSET 에 쌓고 `now-window` 이전을 지우는 방식으로 정확도를 올렸습니다. Lua 로 묶어
  조회·판정·기록이 원자적이라 다중 인스턴스에서도 기준이 흔들리지 않습니다.
- **빠른 실패**: HikariCP `connection-timeout: 3s`, Redis `timeout: 1s` — 스레드가 무한 대기하지 않습니다.
- **스레드/큐 상한**: Tomcat `threads.max=200`, `accept-count=200`, `max-connections=8192`.
  DB 풀(20) 대비 스레드를 과도하게 늘리면 대기열만 길어져 지연이 폭발하므로 의도적으로 보수적으로 잡았습니다.
- **Redis 장애 정책**: `fortune.rate-limit.fail-open: true` — Redis 하나 죽었다고 결제를 막지 않고 통과시키되
  ERROR 로그를 남깁니다(가용성 우선). 반대로 조이려면 `false`(503) 로 바꾸면 됩니다.
- **`maxmemory-policy: noeviction`** — Redis 가 메모리 압박에서 멱등성 키를 evict 하면 이중 결제 방어가 뚫립니다.
  캐시성 데이터는 DB 1번(`redis://redis:6379/1`)에 몰아 두고, 정책은 절대 evict 하지 않도록 했습니다.

### ③ 모듈 간 장애 격리

- 결제 API 는 **Kafka 를 직접 만지지 않습니다**. 이벤트는 `outbox_events` 테이블에 결제와 같은 트랜잭션으로 적재되고,
  `OutboxRelay` 가 비동기로 발행합니다. AI 워커가 죽어 있거나 Kafka 가 느려도 **결제 응답 시간은 그대로**입니다.
- 반대로 Kafka 발행만 실패하면 행이 PENDING 으로 남아 지수 백오프로 재시도하고, 최대 횟수(5)를 넘으면
  `FAILED` 로 남겨 수동 재처리 대상이 됩니다(유실 없음).
- Python 워커는 **주문 테이블을 직접 읽지/쓰지 않습니다.** 상태 변경은 spring-core 내부 API 한 곳으로만 요청합니다.
  스키마가 바뀌어도 워커가 깨지지 않고, 나중에 워커를 다른 언어/팀이 소유해도 경계가 유지됩니다.

### ④ Kafka: 최소 1회 전달 + 멱등 소비

- 프로듀서: `acks=all`, `enable.idempotence=true`(재시도해도 브로커에 한 번만), `lz4` 압축.
  기동 시 설정이 어긋났으면 `KafkaConfig` 가 ERROR 로그를 남깁니다.
- 컨슈머: `enable_auto_commit=false` + **수동 커밋**. 처리 완료 후에만 오프셋을 커밋하므로 처리 중 죽으면 재처리됩니다.
- 동시 처리(8) 중에도 **파티션별로 연속 완료된 지점까지만** 커밋합니다(`OffsetTracker`) — 12·13번이 먼저 끝나도
  11번 전에는 커밋하지 않습니다.
- 같은 이벤트가 두 번 와도 `reading:dedupe:{eventId}`(SET NX) 로 한 번만 반영합니다.
- 3회(최초 포함) 재시도 후 실패하면 **`fortune.paid.dlq`** 로 원본+오류를 감싸 보내고, 오프셋은 전진시켜
  파티션 정체를 막습니다. DLQ 발행마저 실패하면 커밋하지 않아 재기동 시 다시 처리됩니다.

### ⑤ Redis: 토큰/지연 절감

- **동일 입력 캐시**: `reading:cache:{sha256(생년월일시+운세타입+질문)}`. 두 번째 주문이 같은 사람·같은 운세면
  LLM 을 호출하지 않습니다(`cached: true`).
- **조회 API 는 Redis 만 읽습니다** — AI 생성이 오래 걸려도 조회는 즉시 응답합니다.
- 키/TTL 은 Java 와 Python 이 **같은 문자열**을 씁니다(§4 표, `tools/check_consistency.py` 로 자동 검증).

---

## 3. 실행

```bash
cd work/fortune-msa
docker compose up -d --build

# 상태 확인
docker compose ps
curl -s localhost:8080/actuator/health/readiness
curl -s localhost:8000/readyz
```

최초 기동 시 Flyway 가 스키마를 만들고, Kafka 토픽(`fortune.paid`, `fortune.paid.dlq`)이 자동 생성됩니다.
내려갈 때는 `docker compose down`(데이터 유지), `docker compose down -v`(볼륨까지 삭제).

호스트에서 직접 실행할 때의 주소

| 대상 | 컨테이너 내부 | 호스트 |
|---|---|---|
| spring-core | `http://spring-core:8080` | `http://localhost:8080` |
| python-ai-worker | `http://python-ai-worker:8000` | `http://localhost:8000` |
| Kafka | `kafka:9092` | `localhost:29092` |
| PostgreSQL | `postgres:5432` | `localhost:5432` |
| Redis | `redis:6379` | `localhost:6379` |

> Kafka 는 리스너를 두 개 둡니다(내부 9092 / 외부 29092). 호스트에서 `kafka-console-producer` 등을 쓸 때는
> **29092** 를 쓰세요. 9092 는 컨테이너 네트워크 전용입니다.

---

## 4. 계약 표 (바꿀 때 양쪽을 함께 바꾼다)

### 포트 / 토픽

| 항목 | 값 | 정의 위치 |
|---|---|---|
| spring-core | `8080` | `application.yml` `server.port` |
| python-ai-worker | `8000` | `config.yaml` `server.port` |
| PostgreSQL | `5432` | `docker-compose.yml`, `DB_URL` |
| Redis | `6379` | `docker-compose.yml`, `REDIS_HOST/PORT` |
| Kafka | `9092`(내부) / `29092`(호스트) | `docker-compose.yml` |
| 결제 확정 토픽 | `fortune.paid` | `application.yml` `fortune.kafka.topic`, `config.yaml` `kafka.topic` |
| DLQ 토픽 | `fortune.paid.dlq` | 위와 동일 |

### Redis 키

| 키 | 타입 | 쓰는 쪽 | 내용 |
|---|---|---|---|
| `idem:lock:{key}` | string | Java | 멱등 락(토큰, TTL 10s) |
| `idem:resp:{key}` | hash | Java | `requestHash` / `status` / `body` (TTL 24h) |
| `rl:sw:{scope}:{client}` | zset | Java | 슬라이딩 윈도우 요청 시각 |
| `reading:status:{orderId}` | string(JSON) | Java, Python | `orderId,status,attempts,message,updatedAt` |
| `reading:result:{orderId}` | string(JSON) | Python | `orderId,headline,body,model,cached,generatedAt` |
| `reading:cache:{inputHash}` | string(JSON) | Python | LLM 결과 캐시(30일) |
| `reading:dedupe:{eventId}` | string | Python | 컨슈머 중복 방어(7일) |

### `fortune.paid` 메시지

```json
{
  "eventId": "9f1c…",
  "eventType": "fortune.paid",
  "occurredAt": "2026-11-24T01:00:01Z",
  "orderId": "3b0e…",
  "paymentId": "7c2a…",
  "userId": "u-1001",
  "amount": 4900,
  "currency": "KRW",
  "fortuneType": "LOVE",
  "readingInput": {
    "birthDate": "1993-09-13",
    "birthTime": "03:53",
    "calendar": "SOLAR",
    "gender": "M",
    "question": null
  }
}
```

키는 `orderId` 로 고정합니다 → 같은 주문의 이벤트는 같은 파티션에 들어가 순서가 보장됩니다.

### 주요 환경변수

| 변수 | 기본값 | 의미 |
|---|---|---|
| `DB_URL` / `DB_USERNAME` / `DB_PASSWORD` | `jdbc:postgresql://postgres:5432/fortune` / `fortune` / `fortune` | DB 접속 |
| `DB_POOL_SIZE` | `20` | HikariCP 최대 커넥션 |
| `REDIS_HOST` / `REDIS_PORT` | `redis` / `6379` | spring-core Redis(DB 0) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | 브로커 |
| `INTERNAL_API_TOKEN` | `local-internal-token` | 내부 API 인증(운영에서 교체) |
| `RATE_LIMIT_REQUESTS` | `100` | 윈도우(1초)당 허용 수 |
| `FORTUNE_REDIS__URL` | `redis://redis:6379/1` | 워커 Redis(DB 1) |
| `FORTUNE_KAFKA__BOOTSTRAP_SERVERS` | `kafka:9092` | 워커 브로커 |
| `FORTUNE_KAFKA__MAX_CONCURRENCY` | `8` | 동시 처리 메시지 수 |
| `FORTUNE_LLM__PROVIDER` | `mock` | `mock` / `http` |
| `FORTUNE_FAULT_INJECTION__FAIL_FORTUNE_TYPE` | `CRASH_TEST` | DLQ 검증용 장애 주입 |

---

## 5. curl 로 확인하기 (복붙 순서대로)

### 5-1. 주문 생성 (데모용)

```bash
ORDER=$(curl -s -X POST localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{
        "userId": "u-1001",
        "fortuneType": "LOVE",
        "amount": 4900,
        "birthDate": "1993-09-13",
        "birthTime": "03:53",
        "calendarType": "SOLAR",
        "gender": "M"
      }' | python3 -c 'import sys,json;print(json.load(sys.stdin)["orderId"])')
echo "ORDER=$ORDER"
```

### 5-2. 결제 확정

```bash
KEY=$(uuidgen | tr 'A-Z' 'a-z')
curl -s -i -X POST localhost:8080/api/v1/payments/confirm \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $KEY" \
  -d "{\"orderId\":\"$ORDER\",\"amount\":4900,\"currency\":\"KRW\"}"
```

응답 예: `200` / `{"orderId":"…","paymentId":"…","status":"PAID","amount":4900,"currency":"KRW","paidAt":"…","replayed":false}`

### 5-3. **같은 키로 다시** (새로고침 / 중복 클릭 시나리오)

```bash
curl -s -X POST localhost:8080/api/v1/payments/confirm \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $KEY" \
  -d "{\"orderId\":\"$ORDER\",\"amount\":4900}"
```

기대: **`200` + 완전히 같은 `paymentId` + `replayed: true`**, 그리고 DB 결제 행은 여전히 1건.

```bash
docker compose exec postgres psql -U fortune -d fortune \
  -c "select count(*) as payments from payments where order_id = '$ORDER';"
```

### 5-4. 동시에 10번 (처리 중 충돌)

```bash
ORDER2=$(curl -s -X POST localhost:8080/api/v1/orders -H 'Content-Type: application/json' \
  -d '{"userId":"u-1002","fortuneType":"MONEY","amount":9900,"birthDate":"1990-01-01","gender":"F"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["orderId"])')
KEY2=$(uuidgen | tr 'A-Z' 'a-z')
seq 1 10 | xargs -P10 -I{} curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/api/v1/payments/confirm \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY2" \
  -d "{\"orderId\":\"$ORDER2\",\"amount\":9900}" | sort | uniq -c
```

기대: `10x 200`(대부분 재생) 또는 `1x 200 + 9x 409` — **어느 쪽이든 결제는 1건**.

### 5-5. 다른 키로 이미 결제된 주문 다시 → 409

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/api/v1/payments/confirm \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen | tr 'A-Z' 'a-z')" \
  -d "{\"orderId\":\"$ORDER\",\"amount\":4900}"
# 409
```

### 5-6. 같은 키에 다른 금액 → 422

```bash
curl -s -X POST localhost:8080/api/v1/payments/confirm \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $KEY" \
  -d "{\"orderId\":\"$ORDER\",\"amount\":1000}"
# 422 {"code":"IDEMPOTENCY_KEY_REUSED", ...}
```

### 5-7. 운세 결과 조회 (AI 워커)

```bash
curl -s localhost:8000/api/v1/readings/$ORDER | python3 -m json.tool
# {"orderId":"…","status":"COMPLETED","result":{"headline":"…","body":"…","cached":false,…}}
```

같은 사람·같은 운세로 **새 주문**을 결제하면 두 번째는 LLM 을 호출하지 않습니다(`"cached": true`).

### 5-8. DLQ / 재시도 확인 (장애 주입)

```bash
BAD=$(curl -s -X POST localhost:8080/api/v1/orders -H 'Content-Type: application/json' \
  -d '{"userId":"u-1003","fortuneType":"CRASH_TEST","amount":4900,"birthDate":"1993-09-13"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["orderId"])')
curl -s -o /dev/null -X POST localhost:8080/api/v1/payments/confirm \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen | tr 'A-Z' 'a-z')" \
  -d "{\"orderId\":\"$BAD\",\"amount\":4900}"

sleep 8
# 주문은 FAILED, DLQ 에 원본이 남는다
curl -s localhost:8080/api/v1/orders/$BAD | python3 -m json.tool
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fortune.paid.dlq --from-beginning --max-messages 1
```

### 5-9. 레이트 리밋 (101번째부터 429)

```bash
for i in $(seq 1 105); do
  curl -s -o /dev/null -w "%{http_code}\n" localhost:8080/api/v1/orders/$ORDER
done | sort | uniq -c
# 대략 100x 200, 5x 429  (X-RateLimit-Remaining / Retry-After 헤더도 함께 확인)
```

### 5-10. 무슨 일이 있었는지 직접 보기

```bash
docker compose exec redis redis-cli -n 0 keys 'idem:*' | head      # 멱등 기록
docker compose exec redis redis-cli -n 1 keys 'reading:*' | head   # 워커 상태/결과/캐시
docker compose exec postgres psql -U fortune -d fortune \
  -c "select status, count(*) from outbox_events group by 1;"      # PENDING/PUBLISHED
docker compose logs -f python-ai-worker
```

---

## 6. 검증 결과 (이 프로젝트에서 실제로 돌린 것)

| 검증 | 명령 | 결과 |
|---|---|---|
| Java 컴파일 + 단위 테스트 | `cd spring-core && mvn test` | ✅ **10 tests, 0 failures** (결제 재생/금액 불일치/이중결제 차단/이벤트 계약/설정 바인딩) |
| Python 문법 | `python3 -m py_compile`(전체 `app`, `tests`) | ✅ |
| Python 테스트 | `cd python-ai-worker && pytest` | ✅ **15 tests, 0 failures** (재시도 백오프, 오프셋 커밋, 캐시 히트, 중복 이벤트, 장애 주입) |
| YAML 파싱 | `yaml.safe_load` (compose, application.yml, config.yaml) | ✅ |
| 계약 일관성 | `python3 tools/check_consistency.py` | ✅ **65 checks, 0 failures** (토픽/포트/Redis 키/이벤트 필드/환경변수/의존성) |
| Lua 스크립트 | 실제 Redis 6.2 에 Java 소스의 스크립트를 그대로 실행 | ✅ 100건 허용 후 429 판정값(`0:0:989`), 락 compare-and-delete 동작 확인 |
| 워커 경로 | 실제 Redis + FastAPI 조회 API | ✅ 상태 기록→캐시 히트→`GET /api/v1/readings/{id}` 200/404, camelCase 응답 |
| `docker compose up` | 이 작업 환경에 **docker 가 없어 실행하지 못함** | ⚠️ 미실행 — 아래 한계 참고 |

> 이 저장소를 처음 받았다면 위 표의 명령만 그대로 다시 돌려 확인할 수 있습니다.
> (`check_consistency.py` 는 `pip install -r python-ai-worker/requirements.txt` 가 필요합니다.)

---

## 7. 설계 판단과 트레이드오프 (문서/면접용 요약)

1. **멱등성은 Redis 만으로 충분하지 않다.** Redis 는 빠르지만 유실/분할 가능성이 있고, 결제는 그 가능성을
   허용할 수 없는 도메인이다. 그래서 "Redis = 동시성 제어 + 응답 재생", "DB UNIQUE = 최종 진실"로 역할을 나눴다.
2. **아웃박스를 둔 이유는 '결제 응답 시간'이다.** 결제 트랜잭션에서 `KafkaTemplate.send()` 를 직접 부르면
   브로커 지연이 곧 결제 지연이 된다. 같은 트랜잭션에 이벤트를 적재하고 릴레이가 밀어내면 결제는 DB 만 보고 끝난다.
   (파티션이 커지면 같은 패턴을 Debezium CDC 로 대체하는 것이 정석.)
3. **at-least-once 를 전제로 설계했다.** 멱등 프로듀서 + 수동 커밋이어도 재전달은 일어난다.
   그래서 소비자에 `eventId` 중복 방어를 두었고, 이벤트 처리는 "두 번 실행해도 같은 결과"가 되게 작성했다.
4. **DLQ 는 '버리기'가 아니라 '격리'다.** 실패 메시지를 파티션에 붙잡아 두면 뒤의 정상 메시지가 전부 막힌다.
   오프셋을 전진시키고 원본을 DLQ 에 보존해, 운영자가 원인 수정 후 재주입할 수 있게 했다.
5. **공유 DB 대신 내부 API.** MSA 에서 가장 흔한 부패는 "DB 를 공유해서 편해지는 순간"이다.
   워커가 주문 상태를 바꿀 때도 API 를 거치게 해 스키마 결합을 끊었다.
6. **동시성 도구는 두 개를 섞지 않는다.** 결제는 Redis 락으로 직렬화하고, 주문 엔티티에는 `@Version` 낙관적 락을 두어
   다른 경로(관리자 취소 등)의 갱신 충돌을 잡는다.

### 알려진 한계 / 다음 단계

- PG 연동은 `MockPaymentGateway` 입니다(90원 단위 금액은 모의 거절). 실제 PG 는 타임아웃·부분 성공 처리가 필요합니다.
  또한 여기서는 PG 승인 호출을 결제 트랜잭션 **안에서** 합니다(멱등 락이 직렬화를 보장해 단순화한 선택).
  실무에서는 ① PG 승인(외부) → ② 확정 기록(로컬 트랜잭션) 2단계로 나누고, 중간에 죽었을 때를 위해
  PG 조회 API 로 승인 상태를 되짚는 보상 절차를 두는 편이 안전합니다.
- 내부 API 인증은 정적 토큰입니다. 운영에서는 mTLS 또는 단기 서비스 토큰으로 교체해야 합니다.
- 아웃박스 릴레이는 단일 인스턴스 가정의 폴링입니다. 다중 인스턴스에서는 `FOR UPDATE SKIP LOCKED` 또는 리더 선출이 필요합니다.
- 스키마 레지스트리(Avro/Protobuf) 없이 JSON 계약을 씁니다. 필드 **추가**는 하위 호환이지만 이름 변경/삭제는 안 됩니다.
- 관측성은 actuator/health 만 있습니다. 다음 단계로 Micrometer + OpenTelemetry(트레이스에 `orderId` 태그)를 붙이면
  결제→아웃박스→Kafka→워커 구간을 한 줄로 추적할 수 있습니다.
- 부하 테스트(k6/Locust) 시나리오는 아직 없습니다. `5-4` 의 동시 요청 테스트를 k6 로 옮기는 것이 가장 먼저 할 일입니다.

---

## 8. 디렉터리

```
fortune-msa/
├── docker-compose.yml            # postgres · redis · kafka(KRaft) · spring-core · python-ai-worker
├── .env.example
├── README.md
├── tools/check_consistency.py    # 토픽/포트/키/필드/환경변수 계약 자동 점검
├── spring-core/
│   ├── Dockerfile                # 멀티스테이지(maven → jre)
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/fortune/core/
│       │   ├── common/           # ErrorResponse, ApiException, 전역 예외처리, RedisKeys, RequestHasher
│       │   ├── config/           # FortuneProperties, KafkaConfig(토픽+프로듀서 검증)
│       │   ├── idempotency/      # IdempotencyService(락/캐시/DB), Record, 정리 잡
│       │   ├── order/            # Order, OrderService, OrderController, DTO
│       │   ├── payment/          # Payment, PaymentService, PaymentController, Gateway
│       │   ├── event/            # FortunePaidEvent(토픽 계약)
│       │   ├── outbox/           # OutboxEvent/Repository/Service/Relay
│       │   ├── ratelimit/        # 슬라이딩 윈도우 Lua + Filter
│       │   └── internal/         # AI 워커용 상태 변경 API
│       └── resources/
│           ├── application.yml   # Hikari/Tomcat/Kafka/Redis/fortune.* 설정
│           └── db/migration/V1__init.sql
└── python-ai-worker/
    ├── Dockerfile
    ├── requirements.txt
    ├── config.yaml               # kafka/redis/retry/llm/내부 API
    ├── app/
    │   ├── main.py               # FastAPI + lifespan(컨슈머 기동), 조회 API
    │   ├── config.py             # pydantic-settings(yaml+env)
    │   ├── kafka_worker.py       # 수동 커밋/Semaphore/백오프/OffsetTracker
    │   ├── handler.py            # 캐시 히트, 상태 전이, 실패 기록
    │   ├── dlq.py                # DLQ envelope 발행
    │   ├── redis_store.py        # 상태/결과/캐시/중복 저장소(Protocol 로 교체 가능)
    │   ├── keys.py               # Redis 키 규약(Java 와 동일)
    │   ├── schemas.py            # 이벤트/응답 스키마(camelCase)
    │   ├── llm.py                # mock / http LLM 클라이언트
    │   ├── fortune/prompts.py    # 프롬프트 정규화
    │   └── clients/internal_api.py
    └── tests/                    # 백오프·오프셋·핸들러·프롬프트 테스트
```
