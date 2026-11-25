"""fortune.paid 이벤트 처리기.

한 메시지 처리 순서:
  1) eventId 중복 확인 (Redis SET NX) — Kafka 재전달/아웃박스 재발행 대비
  2) 상태 PROCESSING 기록
  3) 장애 주입(mock) 이면 예외 → 워커가 재시도/DLQ 담당
  4) 동일 입력 캐시 히트 → LLM 호출 생략 (토큰/지연 절감)
  5) 결과 저장 + 상태 COMPLETED + spring-core 에 COMPLETED 통지
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from app import keys
from app.config import Settings
from app.llm import LlmClient
from app.redis_store import ReadingStore
from app.schemas import FortunePaidEvent, ReadingResult, ReadingStatus

logger = logging.getLogger(__name__)


class ReadingFailure(RuntimeError):
    """재시도 대상 처리 실패."""


@dataclass
class HandlerResult:
    order_id: str
    status: ReadingStatus
    cache_hit: bool
    skipped: bool = False


class ReadingHandler:
    def __init__(self, settings: Settings, store: ReadingStore, llm: LlmClient, internal_api) -> None:
        self._settings = settings
        self._store = store
        self._llm = llm
        self._internal_api = internal_api

    async def handle(self, event: FortunePaidEvent) -> HandlerResult:
        if await self._store.seen_event(event.event_id):
            logger.info("duplicate event ignored event_id=%s order=%s", event.event_id, event.order_id)
            return HandlerResult(order_id=event.order_id, status=ReadingStatus.PROCESSING, cache_hit=False, skipped=True)

        await self._store.set_status(event.order_id, ReadingStatus.PROCESSING, message="AI 생성 시작")

        reading_input = event.normalised_input()
        if self._settings.fault_injection.fail_fortune_type and (
            event.fortune_type == self._settings.fault_injection.fail_fortune_type
        ):
            raise ReadingFailure("fault injection: forced failure for fortune_type=" + event.fortune_type)

        cache_hit = False
        result: ReadingResult | None = None
        cache_key_hash = keys.input_hash(reading_input, event.fortune_type)

        if self._settings.llm.cache_enabled:
            cached = await self._store.get_cached_result(cache_key_hash)
            if cached is not None:
                cache_hit = True
                result = cached.model_copy(
                    update={
                        "order_id": event.order_id,
                        "cached": True,
                        "generated_at": datetime.now(timezone.utc),
                    }
                )
                logger.info("cache hit order=%s input_hash=%s", event.order_id, cache_key_hash)

        if result is None:
            headline, body = await self._llm.generate(reading_input, event.fortune_type)
            if not headline or not body:
                raise ReadingFailure("LLM returned empty content")
            result = ReadingResult(
                order_id=event.order_id,
                headline=headline,
                body=body,
                model=self._llm.model,
                cached=False,
                generated_at=datetime.now(timezone.utc),
            )
            if self._settings.llm.cache_enabled:
                await self._store.put_cached_result(cache_key_hash, result)

        await self._store.put_result(result)
        await self._store.set_status(event.order_id, ReadingStatus.COMPLETED, message="AI 생성 완료")
        await self._internal_api.update_order_status(event.order_id, "COMPLETED", attempts=1)
        return HandlerResult(order_id=event.order_id, status=ReadingStatus.COMPLETED, cache_hit=cache_hit)

    async def on_exhausted(self, event: FortunePaidEvent, error: BaseException, attempts: int) -> None:
        """재시도를 모두 소진했을 때: 상태 FAILED 기록 + 주문 FAILED 통지."""
        message = f"{type(error).__name__}: {error}"[:500]
        try:
            # DLQ 에서 원인 수정 후 재주입할 수 있도록 중복 방어 키를 해제한다.
            await self._store.clear_event(event.event_id)
        except Exception:
            logger.exception("failed to clear dedupe key event=%s", event.event_id)
        try:
            await self._store.set_status(event.order_id, ReadingStatus.FAILED, attempts=attempts, message=message)
        except Exception:  # Redis 장애가 DLQ 발행까지 막지 않게
            logger.exception("failed to record FAILED status order=%s", event.order_id)
        await self._internal_api.update_order_status(event.order_id, "FAILED", reason=message, attempts=attempts)
