"""spring-core 내부 API 클라이언트.

AI 워커는 주문 테이블을 직접 만지지 않는다(공유 DB 안티패턴 회피).
주문 상태 변경은 spring-core 의 ``PATCH /internal/v1/orders/{orderId}/status`` 로만 요청한다.
이 호출이 실패해도 워커의 본 처리(결과 저장)는 성공으로 본다 — 상태 동기화는 재시도/보상 대상이며,
사용자가 보는 조회 API 는 Redis 상태를 읽기 때문이다.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

import httpx

from app.config import InternalApiSettings
from app.schemas import ReadingStatus

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {ReadingStatus.COMPLETED, ReadingStatus.FAILED}


class InternalApiClient:
    def __init__(self, settings: InternalApiSettings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            base_url=settings.base_url,
            timeout=settings.timeout_seconds,
            headers={"X-Internal-Token": settings.token, "Content-Type": "application/json"},
        )

    async def update_order_status(
        self,
        order_id: str,
        status: Literal["COMPLETED", "FAILED"],
        *,
        reason: str | None = None,
        attempts: int = 0,
        max_attempts: int = 2,
    ) -> bool:
        payload = {"status": status, "reason": reason, "attempts": attempts}
        for attempt in range(1, max_attempts + 1):
            try:
                resp = await self._client.patch(f"/internal/v1/orders/{order_id}/status", json=payload)
                if resp.status_code < 300:
                    return True
                if resp.status_code in (401, 403):
                    logger.error("internal api rejected token (status=%s) order=%s", resp.status_code, order_id)
                    return False
                logger.warning(
                    "internal api returned %s for order=%s (attempt %s)", resp.status_code, order_id, attempt
                )
            except httpx.HTTPError as exc:
                logger.warning("internal api call failed for order=%s: %s (attempt %s)", order_id, exc, attempt)
            if attempt < max_attempts:
                await asyncio.sleep(0.2 * attempt)
        return False

    async def close(self) -> None:
        await self._client.aclose()


class InMemoryInternalApiClient:
    """테스트용 레코더."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []

    async def update_order_status(
        self,
        order_id: str,
        status: Literal["COMPLETED", "FAILED"],
        *,
        reason: str | None = None,
        attempts: int = 0,
        max_attempts: int = 2,
    ) -> bool:
        self.calls.append((order_id, status, reason))
        return True

    async def close(self) -> None:
        return None
