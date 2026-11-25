#!/usr/bin/env python3
"""구성 일관성 점검 스크립트.

Java(spring-core) · Python(python-ai-worker) · docker-compose 가 **같은 토픽명, 포트, Redis 키,
이벤트 필드 계약, 환경변수 이름**을 쓰는지 한 번에 확인한다. 서비스 간 계약이 깨지는 사고의 대부분이
여기서 잡힌다.

사용법:
    # 저장소 루트(work/fortune-msa)에서
    python3 tools/check_consistency.py

    python-ai-worker 의 requirements 가 설치된 인터프리터로 실행해야 한다(설정/스키마 모델 import 때문):
    pip install -r python-ai-worker/requirements.txt
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
JAVA_MAIN = ROOT / "spring-core/src/main/java/com/fortune/core"
PY_WORKER = ROOT / "python-ai-worker"

failures: list[str] = []
checks = 0


def ok(label: str, detail: str = "") -> None:
    global checks
    checks += 1
    print(f"  PASS  {label}" + (f"  ({detail})" if detail else ""))


def fail(label: str, detail: str,) -> None:
    global checks
    checks += 1
    failures.append(f"{label}: {detail}")
    print(f"  FAIL  {label}  → {detail}")


def expect_equal(label: str, actual, expected) -> None:
    if actual == expected:
        ok(label, repr(actual))
    else:
        fail(label, f"actual={actual!r} expected={expected!r}")


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def java_record_components(relative_path: str, record_name: str) -> list[str]:
    """Java record 의 컴포넌트 이름 목록(선언 순서)."""
    source = read(JAVA_MAIN / relative_path)
    match = re.search(rf"public record {record_name}\(", source)
    if not match:
        raise SystemExit(f"record {record_name} not found in {relative_path}")
    start = match.end() - 1
    depth = 0
    for index in range(start, len(source)):
        char = source[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                params = source[start + 1:index]
                break
    else:
        raise SystemExit(f"unbalanced parentheses for {record_name}")

    params = re.sub(r"@\w+(\([^)]*\))?", "", params)  # 어노테이션 제거
    components: list[str] = []
    depth = 0
    current = ""
    for char in params:
        if char in "<(":
            depth += 1
        elif char in ">)":
            depth -= 1
        if char == "," and depth == 0:
            components.append(current)
            current = ""
        else:
            current += char
    if current.strip():
        components.append(current)
    return [part.split()[-1].strip() for part in components if part.strip()]


def java_string_constants(path: Path) -> dict[str, str]:
    source = read(path)
    return dict(re.findall(r'String\s+(\w+)\s*=\s*"([^"]+)";', source))


def main() -> int:
    sys.path.insert(0, str(PY_WORKER))
    from app import keys as py_keys  # noqa: PLC0415
    from app.config import Settings  # noqa: PLC0415
    from app.schemas import FortunePaidEvent as PyEvent  # noqa: PLC0415
    from app.schemas import ReadingInput as PyInput  # noqa: PLC0415
    from app.schemas import ReadingResult as PyResult  # noqa: PLC0415
    from app.schemas import ReadingStatusRecord as PyStatus  # noqa: PLC0415

    compose = yaml.safe_load(read(ROOT / "docker-compose.yml"))
    app_yml = yaml.safe_load(read(ROOT / "spring-core/src/main/resources/application.yml"))
    worker_cfg = yaml.safe_load(read(PY_WORKER / "config.yaml"))

    print("[1] YAML 파싱")
    ok("docker-compose.yml", ", ".join(compose["services"].keys()))
    ok("application.yml", ", ".join(app_yml.keys()))
    ok("config.yaml", ", ".join(worker_cfg.keys()))

    print("[2] Kafka 토픽 / 브로커")
    topic = app_yml["fortune"]["kafka"]["topic"]
    dlq_topic = app_yml["fortune"]["kafka"]["dlq-topic"]
    expect_equal("topic == python config", worker_cfg["kafka"]["topic"], topic)
    expect_equal("dlq topic == python config", worker_cfg["kafka"]["dlq_topic"], dlq_topic)
    expect_equal("topic == compose env", compose["services"]["python-ai-worker"]["environment"]["FORTUNE_KAFKA__TOPIC"], topic)
    expect_equal("dlq == compose env", compose["services"]["python-ai-worker"]["environment"]["FORTUNE_KAFKA__DLQ_TOPIC"], dlq_topic)
    expect_equal("spring bootstrap 기본값", app_yml["spring"]["kafka"]["bootstrap-servers"], "${KAFKA_BOOTSTRAP_SERVERS:kafka:9092}")
    expect_equal("compose spring bootstrap", compose["services"]["spring-core"]["environment"]["KAFKA_BOOTSTRAP_SERVERS"], "kafka:9092")
    expect_equal("compose worker bootstrap", compose["services"]["python-ai-worker"]["environment"]["FORTUNE_KAFKA__BOOTSTRAP_SERVERS"], "kafka:9092")
    expect_equal("kafka 내부 포트", compose["services"]["kafka"]["environment"]["KAFKA_ADVERTISED_LISTENERS"], "PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092")
    if any(p.startswith("9092:") for p in compose["services"]["kafka"].get("ports", [])):
        fail("kafka host port", "9092 를 호스트에 매핑하면 PLAINTEXT advertised 와 충돌한다 (29092 사용)")
    else:
        ok("kafka host port", "29092 (내부 9092 와 분리)")

    print("[3] 포트")
    expect_equal("spring server.port", app_yml["server"]["port"], "${SERVER_PORT:8080}")
    expect_equal("spring compose port", compose["services"]["spring-core"]["ports"], ["8080:8080"])
    expect_equal("worker port(config)", worker_cfg["server"]["port"], 8000)
    expect_equal("worker compose port", compose["services"]["python-ai-worker"]["ports"], ["8000:8000"])
    expect_equal("postgres port", compose["services"]["postgres"]["ports"], ["5432:5432"])
    expect_equal("redis port", compose["services"]["redis"]["ports"], ["6379:6379"])
    expect_equal("jdbc url 포트", app_yml["spring"]["datasource"]["url"], "${DB_URL:jdbc:postgresql://postgres:5432/fortune}")

    print("[4] Redis 키 규약 (Java ↔ Python)")
    java_keys = java_string_constants(JAVA_MAIN / "common/RedisKeys.java")
    py_constants = {
        "reading:status:": py_keys.READING_STATUS_PREFIX,
        "reading:result:": py_keys.READING_RESULT_PREFIX,
        "reading:cache:": py_keys.READING_CACHE_PREFIX,
        "reading:dedupe:": py_keys.READING_DEDUPE_PREFIX,
    }
    # 양쪽이 함께 쓰는 키: 문자열이 같아야 한다.
    expect_equal("reading:status 공유", java_keys["READING_STATUS_PREFIX"], py_constants["reading:status:"])
    expect_equal("reading:result 공유", java_keys["READING_RESULT_PREFIX"], py_constants["reading:result:"])
    # Java 전용 키
    expect_equal("idem:lock", java_keys["IDEMPOTENCY_LOCK_PREFIX"], "idem:lock:")
    expect_equal("idem:resp", java_keys["IDEMPOTENCY_RESPONSE_PREFIX"], "idem:resp:")
    expect_equal("rl:sw", java_keys["RATE_LIMIT_PREFIX"], "rl:sw:")
    # Python 전용 키 (캐시/중복방어)
    expect_equal("reading:cache", py_constants["reading:cache:"], "reading:cache:")
    expect_equal("reading:dedupe", py_constants["reading:dedupe:"], "reading:dedupe:")
    key_builder_check = [
        ("status", py_keys.status_key("abc"), "reading:status:abc"),
        ("result", py_keys.result_key("abc"), "reading:result:abc"),
        ("cache", py_keys.cache_key("abc"), "reading:cache:abc"),
        ("dedupe", py_keys.dedupe_key("abc"), "reading:dedupe:abc"),
    ]
    for label, actual, expected in key_builder_check:
        expect_equal(f"키 생성 {label}", actual, expected)

    print("[5] 이벤트 필드 계약 (fortune.paid)")
    def wire_names(model) -> list[str]:
        """JSON 에 실제로 나가는 필드 이름(alias 우선)."""
        return [field.alias or name for name, field in model.model_fields.items()]

    java_event = java_record_components("event/FortunePaidEvent.java", "FortunePaidEvent")
    expect_equal("fortune.paid 필드", wire_names(PyEvent), java_event)
    java_input = java_record_components("event/FortunePaidEvent.java", "ReadingInput")
    expect_equal("readingInput 필드", wire_names(PyInput), java_input)
    expect_equal("eventType 상수", PyEvent.model_fields["event_type"].default, "fortune.paid")

    print("[6] Redis 상태/결과 JSON 필드 계약")
    expect_equal("reading:status 필드",
                 list(PyStatus.model_fields.keys()),
                 ["order_id", "status", "attempts", "message", "updated_at"])
    expect_equal("reading:result 필드",
                 list(PyResult.model_fields.keys()),
                 ["order_id", "headline", "body", "model", "cached", "generated_at"])
    order_service = read(JAVA_MAIN / "order/OrderService.java")
    for field in ('"orderId"', '"status"', '"attempts"', '"message"', '"updatedAt"'):
        if field in order_service:
            ok(f"Java 가 쓰는 상태 필드 {field}")
        else:
            fail(f"Java 상태 필드 {field}", "OrderService 에서 발견되지 않음")

    print("[7] compose 환경변수 ↔ 코드 설정 이름")
    settings = Settings()
    worker_env = compose["services"]["python-ai-worker"]["environment"]
    for name in worker_env:
        if not name.startswith("FORTUNE_"):
            continue
        path = name[len("FORTUNE_"):].lower().split("__")
        target = settings
        for attribute in path:
            if hasattr(target, attribute):
                target = getattr(target, attribute)
            else:
                fail(f"env {name}", f"설정 경로 {'/'.join(path)} 없음")
                break
        else:
            ok(f"env {name}", " → ".join(path))
    spring_env = compose["services"]["spring-core"]["environment"]
    for name in ("DB_URL", "REDIS_HOST", "REDIS_PORT", "KAFKA_BOOTSTRAP_SERVERS", "INTERNAL_API_TOKEN"):
        if f"${{{name}" in read(ROOT / "spring-core/src/main/resources/application.yml"):
            ok(f"env {name}", "application.yml 에서 참조됨")
        else:
            fail(f"env {name}", "application.yml 에서 사용되지 않음")

    print("[8] 서비스 주소(컨테이너 DNS) ↔ compose 서비스명")
    jdbc_url = re.sub(r".*//", "", app_yml["spring"]["datasource"]["url"])  # ${DB_URL:jdbc:postgresql://postgres:5432/fortune}
    hosts = {
        "kafka": worker_cfg["kafka"]["bootstrap_servers"].split(":")[0],
        "redis": re.sub(r"redis://([^:/]+).*", r"\1", worker_cfg["redis"]["url"]),
        "spring-core": re.sub(r"https?://([^:/]+).*", r"\1", worker_cfg["internal_api"]["base_url"]),
        "postgres": jdbc_url.split(":")[0],
    }
    for service, host in hosts.items():
        if host in compose["services"]:
            ok(f"{service} host", host)
        else:
            fail(f"{service} host", f"{host} 가 compose services 에 없음")

    print("[9] 필수 서비스/의존성")
    for service in ("postgres", "redis", "kafka", "spring-core", "python-ai-worker"):
        if service not in compose["services"]:
            fail("service", service)
            continue
        depends = compose["services"][service].get("depends_on", {})
        if service in ("spring-core", "python-ai-worker"):
            if all(isinstance(value, dict) and value.get("condition") == "service_healthy" for value in depends.values()):
                ok(f"{service} depends_on healthy", str(list(depends)))
            else:
                fail(f"{service} depends_on", "condition: service_healthy 필요")
        if "healthcheck" not in compose["services"][service]:
            fail(f"{service} healthcheck", "없음")
        else:
            ok(f"{service} healthcheck")

    print("[10] Redis 정책")
    redis_cmd = compose["services"]["redis"]["command"]
    if "noeviction" in redis_cmd:
        ok("maxmemory-policy", "noeviction (멱등성 키 보호)")
    else:
        fail("maxmemory-policy", "noeviction 이어야 멱등성 키가 evict 되지 않는다")

    print()
    print(f"총 {checks}개 점검, 실패 {len(failures)}개")
    if failures:
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("모든 계약이 일치합니다.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
