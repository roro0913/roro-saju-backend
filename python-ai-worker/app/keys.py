"""Redis 키 규약.

spring-core(Java) 의 ``com.fortune.core.common.RedisKeys`` 와 **반드시 동일한 문자열**을 사용한다.
키를 바꿀 때는 양쪽을 함께 바꾼다. (README 의 'Redis 키 계약' 표 참고)

    reading:status:{orderId}  -> JSON  {"orderId","status","attempts","message","updatedAt"}
    reading:result:{orderId}  -> JSON  {"orderId","headline","body","model","cached","generatedAt"}
    reading:cache:{inputHash} -> JSON  (LLM 결과 캐시, orderId 무관 - 동일 입력 재사용)
    reading:dedupe:{eventId}  -> "1"   (Kafka 중복 전달 방어)
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

READING_STATUS_PREFIX = "reading:status:"
READING_RESULT_PREFIX = "reading:result:"
READING_CACHE_PREFIX = "reading:cache:"
READING_DEDUPE_PREFIX = "reading:dedupe:"


def status_key(order_id: str) -> str:
    return f"{READING_STATUS_PREFIX}{order_id}"


def result_key(order_id: str) -> str:
    return f"{READING_RESULT_PREFIX}{order_id}"


def cache_key(input_hash: str) -> str:
    return f"{READING_CACHE_PREFIX}{input_hash}"


def dedupe_key(event_id: str) -> str:
    return f"{READING_DEDUPE_PREFIX}{event_id}"


def canonical_json(payload: dict[str, Any]) -> str:
    """키 정규화: dict 순서/공백 차이를 제거해 같은 입력은 같은 문자열이 되게 한다."""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def input_hash(reading_input: dict[str, Any], fortune_type: str) -> str:
    """생년월일시 + 운세 타입 기반 캐시 키 (sha256, 32자)."""
    material = canonical_json({"input": reading_input, "fortuneType": fortune_type})
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:32]
