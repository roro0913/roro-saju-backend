"""LLM 클라이언트.

- ``MockLlmClient``: 외부 호출 없이 입력 해시로 결정적인 결과를 만든다(로컬/테스트/캐시 검증용).
- ``HttpLlmClient``: 사내 LLM 게이트웨이 등 HTTP 엔드포인트를 호출한다. 실패 시 예외를 던져
  워커의 재시도 → DLQ 경로를 그대로 타게 한다.

새 모델을 붙일 때는 ``LlmClient`` 프로토콜만 만족시키면 되고, 워커/핸들러는 손대지 않아도 된다.
"""

from __future__ import annotations

import hashlib
from typing import Any, Protocol

import httpx

from app.config import LlmSettings
from app.fortune.prompts import build_user_prompt


class LlmError(RuntimeError):
    """LLM 호출 실패(재시도 대상)."""


class LlmClient(Protocol):
    model: str

    async def generate(self, reading_input: dict[str, Any], fortune_type: str) -> tuple[str, str]:
        """(headline, body) 를 돌려준다."""
        ...


class MockLlmClient:
    """결정적 목 클라이언트. 같은 입력 → 같은 출력이라 캐시 히트 검증에 적합하다."""

    def __init__(self, model: str = "fortune-mock-v1") -> None:
        self.model = model

    async def generate(self, reading_input: dict[str, Any], fortune_type: str) -> tuple[str, str]:
        prompt = build_user_prompt(reading_input, fortune_type)
        digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
        seed = int(digest[:8], 16)
        themes = ["균형", "확장", "정리", "점검", "전환", "수확"]
        theme = themes[seed % len(themes)]
        headline = f"{fortune_type} · {theme}의 흐름"
        body = (
            f"지금의 중심에는 '{theme}'이 있습니다. {reading_input.get('birthDate', '생년월일')} 기준으로 "
            f"보면 기운이 안정적으로 자리 잡는 구간이라, 무리한 확장보다는 손에 쥔 것을 다지는 편이 낫습니다.\n\n"
            "관계에서는 말보다 태도가 먼저 읽히는 시기입니다. 약속을 작게 잡고 정확히 지키면 신뢰가 쌓입니다. "
            "금전은 새 지출을 늘리기보다 고정비를 한 번 정리할 때 효과가 큽니다.\n\n"
            "결론적으로 오늘은 '결정을 미루는 대신 순서를 정하는' 하루가 좋습니다. "
            f"(mock 응답 · seed={seed % 10000:04d})"
        )
        return headline, body


class HttpLlmClient:
    """HTTP 기반 LLM 호출. 응답 스키마는 {"headline": str, "body": str} 를 기대한다."""

    def __init__(self, settings: LlmSettings) -> None:
        if not settings.endpoint:
            raise ValueError("llm.endpoint is required when provider=http")
        self.model = settings.model
        self._settings = settings

    async def generate(self, reading_input: dict[str, Any], fortune_type: str) -> tuple[str, str]:
        payload = {
            "model": self._settings.model,
            "temperature": self._settings.temperature,
            "messages": [
                {"role": "system", "content": build_user_prompt(reading_input, fortune_type)},
                {"role": "user", "content": "운세를 작성해줘."},
            ],
        }
        headers = {"Content-Type": "application/json"}
        if self._settings.api_key:
            headers["Authorization"] = f"Bearer {self._settings.api_key}"
        try:
            async with httpx.AsyncClient(timeout=self._settings.timeout_seconds) as client:
                resp = await client.post(self._settings.endpoint, json=payload, headers=headers)
                resp.raise_for_status()
                data = resp.json()
        except (httpx.HTTPError, ValueError) as exc:  # 타임아웃/5xx/JSON 오류 모두 재시도 대상
            raise LlmError(f"LLM call failed: {exc}") from exc

        headline = str(data.get("headline") or "").strip()
        body = str(data.get("body") or "").strip()
        if not headline or not body:
            raise LlmError("LLM response missing headline/body")
        return headline, body


def build_llm_client(settings: LlmSettings) -> LlmClient:
    if settings.provider == "http" and settings.endpoint:
        return HttpLlmClient(settings)
    return MockLlmClient(settings.model)
