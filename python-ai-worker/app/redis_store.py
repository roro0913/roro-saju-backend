"""Redis 저장소: 처리 상태 / 결과 / LLM 캐시 / 중복 방어.

핸들러는 ``ReadingStore`` 프로토콜에만 의존하므로 테스트에서는 메모리 구현으로 바꿔 끼울 수 있다.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol

from redis.asyncio import Redis

from app import keys
from app.config import RedisSettings
from app.schemas import ReadingResult, ReadingStatus, ReadingStatusRecord


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ReadingStore(Protocol):
    async def seen_event(self, event_id: str) -> bool: ...

    async def clear_event(self, event_id: str) -> None: ...

    async def set_status(
        self, order_id: str, status: ReadingStatus, *, attempts: int = 0, message: str | None = None
    ) -> None: ...

    async def get_status(self, order_id: str) -> ReadingStatusRecord | None: ...

    async def get_cached_result(self, input_hash: str) -> ReadingResult | None: ...

    async def put_cached_result(self, input_hash: str, result: ReadingResult) -> None: ...

    async def put_result(self, result: ReadingResult) -> None: ...

    async def get_result(self, order_id: str) -> ReadingResult | None: ...


class RedisReadingStore:
    def __init__(self, redis: Redis, settings: RedisSettings) -> None:
        self._redis = redis
        self._settings = settings

    # --- 중복 전달 방어 -------------------------------------------------
    async def seen_event(self, event_id: str) -> bool:
        """이미 처리한 eventId 면 True. SET NX 로 선점해 동시 중복도 한 번만 통과시킨다."""
        acquired = await self._redis.set(
            keys.dedupe_key(event_id), "1", nx=True, ex=self._settings.dedupe_ttl_seconds
        )
        return acquired is None

    async def clear_event(self, event_id: str) -> None:
        """처리에 최종 실패했을 때 중복 방어 키를 지운다 → DLQ 에서 재주입하면 다시 처리된다."""
        await self._redis.delete(keys.dedupe_key(event_id))

    # --- 처리 상태 -----------------------------------------------------
    async def set_status(
        self, order_id: str, status: ReadingStatus, *, attempts: int = 0, message: str | None = None
    ) -> None:
        record = ReadingStatusRecord(
            order_id=order_id, status=status, attempts=attempts, message=message, updated_at=_now()
        )
        await self._redis.set(
            keys.status_key(order_id),
            record.model_dump_json(by_alias=True),
            ex=self._settings.reading_ttl_seconds,
        )

    async def get_status(self, order_id: str) -> ReadingStatusRecord | None:
        raw = await self._redis.get(keys.status_key(order_id))
        if raw is None:
            return None
        return ReadingStatusRecord.model_validate_json(raw)

    # --- LLM 캐시 ------------------------------------------------------
    async def get_cached_result(self, input_hash: str) -> ReadingResult | None:
        raw = await self._redis.get(keys.cache_key(input_hash))
        if raw is None:
            return None
        return ReadingResult.model_validate_json(raw)

    async def put_cached_result(self, input_hash: str, result: ReadingResult) -> None:
        await self._redis.set(
            keys.cache_key(input_hash),
            result.model_dump_json(by_alias=True),
            ex=self._settings.cache_ttl_seconds,
        )

    # --- 주문별 결과 ----------------------------------------------------
    async def put_result(self, result: ReadingResult) -> None:
        await self._redis.set(
            keys.result_key(result.order_id),
            result.model_dump_json(by_alias=True),
            ex=self._settings.reading_ttl_seconds,
        )

    async def get_result(self, order_id: str) -> ReadingResult | None:
        raw = await self._redis.get(keys.result_key(order_id))
        if raw is None:
            return None
        return ReadingResult.model_validate_json(raw)

    async def close(self) -> None:
        await self._redis.aclose()


class InMemoryReadingStore:
    """테스트용. 프로세스 메모리에만 저장한다."""

    def __init__(self, ttl_seconds: int = 60) -> None:
        self._status: dict[str, str] = {}
        self._results: dict[str, str] = {}
        self._cache: dict[str, str] = {}
        self._seen: set[str] = set()
        self.ttl_seconds = ttl_seconds

    async def seen_event(self, event_id: str) -> bool:
        if event_id in self._seen:
            return True
        self._seen.add(event_id)
        return False

    async def clear_event(self, event_id: str) -> None:
        self._seen.discard(event_id)

    async def set_status(
        self, order_id: str, status: ReadingStatus, *, attempts: int = 0, message: str | None = None
    ) -> None:
        self._status[order_id] = ReadingStatusRecord(
            order_id=order_id, status=status, attempts=attempts, message=message, updated_at=_now()
        ).model_dump_json(by_alias=True)

    async def get_status(self, order_id: str) -> ReadingStatusRecord | None:
        raw = self._status.get(order_id)
        return ReadingStatusRecord.model_validate_json(raw) if raw else None

    async def get_cached_result(self, input_hash: str) -> ReadingResult | None:
        raw = self._cache.get(input_hash)
        return ReadingResult.model_validate_json(raw) if raw else None

    async def put_cached_result(self, input_hash: str, result: ReadingResult) -> None:
        self._cache[input_hash] = result.model_dump_json(by_alias=True)

    async def put_result(self, result: ReadingResult) -> None:
        self._results[result.order_id] = result.model_dump_json(by_alias=True)

    async def get_result(self, order_id: str) -> ReadingResult | None:
        raw = self._results.get(order_id)
        return ReadingResult.model_validate_json(raw) if raw else None

    async def close(self) -> None:
        return None


def create_redis(settings: RedisSettings) -> Redis:
    return Redis.from_url(settings.url, decode_responses=True, health_check_interval=30)
