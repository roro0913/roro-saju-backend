"""fortune.paid 컨슈머.

핵심 설계
- **수동 커밋**: 처리가 끝난 오프셋만 커밋한다. 처리 중 프로세스가 죽으면 재처리되며,
  eventId 중복 방어(Redis SET NX)로 부작용이 두 번 생기지 않는다.
- **동시성 제어**: ``asyncio.Semaphore(max_concurrency)`` 로 in-flight 메시지 수를 제한한다.
  LLM 호출은 I/O 바운드라 이 값이 곧 처리량 상한이 된다(무제한 동시 호출로 인한 토큰 폭주 방지).
- **순서 보장 커밋**: 동시 처리 시 오프셋이 뒤죽박죽 완료되므로, 파티션별로 **연속 완료된 지점까지만** 커밋한다.
- **지수 백오프 재시도**: 재시도 실패가 누적되면 DLQ 로 격리하고 오프셋은 전진시켜 파티션 정체를 막는다.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, Callable

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord, OffsetAndMetadata

from app.config import KafkaSettings, RetrySettings
from app.dlq import DlqPublisher
from app.handler import ReadingHandler
from app.schemas import FortunePaidEvent

logger = logging.getLogger(__name__)


def compute_backoff_ms(
    attempt: int,
    settings: RetrySettings,
    rand: Callable[[], float] = random.random,
) -> int:
    """attempt(1부터) 번째 재시도 전 대기 시간(ms). 지수 증가 + 지터."""
    if attempt < 1:
        attempt = 1
    base = settings.initial_backoff_ms * (settings.multiplier ** (attempt - 1))
    base = min(base, settings.max_backoff_ms)
    if settings.jitter_ratio <= 0:
        return int(base)
    jitter = base * settings.jitter_ratio * (2 * rand() - 1)  # ±jitter_ratio
    return max(0, int(base + jitter))


class OffsetTracker:
    """파티션별로 '끊김 없이 완료된' 오프셋까지만 커밋 가능하게 계산한다.

    동시 처리 때문에 12, 13 이 먼저 끝나도 11 이 끝나기 전에는 커밋하지 않는다.
    반대로 이미 커밋한 지점보다 뒤로는 절대 되돌리지 않는다(중복 커밋으로 인한 재처리 방지).
    """

    def __init__(self) -> None:
        self._completed: dict[TopicPartition, set[int]] = {}
        self._next_expected: dict[TopicPartition, int] = {}
        self._committed_upto: dict[TopicPartition, int] = {}
        self._committable: dict[TopicPartition, int] = {}

    def mark_completed(self, tp: TopicPartition, offset: int) -> None:
        if tp not in self._next_expected:
            self._next_expected[tp] = offset
            self._committed_upto[tp] = offset - 1
        done = self._completed.setdefault(tp, set())
        done.add(offset)
        expected = self._next_expected[tp]
        while expected in done:
            done.discard(expected)
            expected += 1
        self._next_expected[tp] = expected
        last_contiguous = expected - 1
        if last_contiguous > self._committed_upto[tp]:
            self._committed_upto[tp] = last_contiguous
            self._committable[tp] = last_contiguous

    def drain(self) -> dict[TopicPartition, OffsetAndMetadata]:
        """커밋 대상(다음에 읽어야 할 오프셋) 스냅샷을 돌려주고 비운다."""
        commits = {
            tp: OffsetAndMetadata(offset=last + 1, metadata="")
            for tp, last in self._committable.items()
            if last >= 0
        }
        self._committable.clear()
        return commits


class FortuneConsumer:
    def __init__(
        self,
        settings: KafkaSettings,
        retry: RetrySettings,
        handler: ReadingHandler,
        dlq: DlqPublisher,
        consumer: AIOKafkaConsumer | None = None,
    ) -> None:
        self._settings = settings
        self._retry = retry
        self._handler = handler
        self._dlq = dlq
        self._consumer = consumer or AIOKafkaConsumer(
            settings.topic,
            bootstrap_servers=settings.bootstrap_servers,
            group_id=settings.group_id,
            enable_auto_commit=settings.enable_auto_commit,
            auto_offset_reset=settings.auto_offset_reset,
            session_timeout_ms=settings.session_timeout_ms,
            max_poll_interval_ms=settings.max_poll_interval_ms,
            client_id="fortune-ai-worker",
        )
        self._semaphore = asyncio.Semaphore(settings.max_concurrency)
        self._tracker = OffsetTracker()
        self._commit_lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._started = False
        self._task: asyncio.Task | None = None
        self._inFlight: set[asyncio.Task] = set()

    # --- lifecycle ----------------------------------------------------
    async def start(self) -> None:
        await self._consumer.start()
        self._started = True
        self._task = asyncio.create_task(self._run(), name="fortune-consumer")
        logger.info(
            "consumer started topic=%s group=%s concurrency=%s",
            self._settings.topic,
            self._settings.group_id,
            self._settings.max_concurrency,
        )

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001 - 종료 경로
                pass
        if self._inFlight:
            await asyncio.gather(*self._inFlight, return_exceptions=True)
        try:
            if self._started:
                await self._flush_commits()
        finally:
            # start() 가 실패한 경우(브로커 다운)에는 정지 호출도 건너뛴다.
            if self._started:
                await self._consumer.stop()
                self._started = False
            logger.info("consumer stopped")

    # --- main loop ----------------------------------------------------
    async def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                batches = await self._consumer.getmany(
                    timeout_ms=self._settings.poll_timeout_ms,
                    max_records=self._settings.max_poll_records,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 브로커 일시 장애
                logger.warning("poll failed: %s", exc)
                await asyncio.sleep(1.0)
                continue

            for tp, records in batches.items():
                for record in records:
                    task = asyncio.create_task(self._process(tp, record))
                    self._inFlight.add(task)
                    task.add_done_callback(self._inFlight.discard)

            if self._inFlight:
                await asyncio.gather(*list(self._inFlight), return_exceptions=True)
            await self._flush_commits()

    async def _flush_commits(self) -> None:
        commits = self._tracker.drain()
        if not commits:
            return
        async with self._commit_lock:
            try:
                await self._consumer.commit(commits)
            except Exception as exc:  # noqa: BLE001
                # 커밋 실패 시 재처리될 뿐이므로 유실은 없다(중복 방어는 eventId 로).
                logger.warning("offset commit failed: %s", exc)

    # --- single record ------------------------------------------------
    async def _process(self, tp: TopicPartition, record: ConsumerRecord) -> None:
        async with self._semaphore:
            raw_key = record.key.decode("utf-8") if record.key else None
            try:
                payload: dict[str, Any] = json.loads(record.value)
            except (ValueError, TypeError) as exc:
                logger.error("invalid message JSON partition=%s offset=%s", tp.partition, record.offset)
                if await self._safe_publish_dlq(None, raw_key, exc, 0, tp, record.offset):
                    self._tracker.mark_completed(tp, record.offset)
                return

            try:
                event = FortunePaidEvent.model_validate(payload)
            except Exception as exc:  # noqa: BLE001 - 스키마 위반은 재시도 무의미
                logger.error("schema violation: %s", exc)
                if await self._safe_publish_dlq(payload, raw_key, exc, 0, tp, record.offset):
                    self._tracker.mark_completed(tp, record.offset)
                return

            attempt = 0
            last_error: BaseException | None = None
            while attempt < self._retry.max_attempts:
                attempt += 1
                try:
                    result = await self._handler.handle(event)
                    logger.info(
                        "processed order=%s attempt=%s cache_hit=%s skipped=%s",
                        result.order_id,
                        attempt,
                        result.cache_hit,
                        result.skipped,
                    )
                    self._tracker.mark_completed(tp, record.offset)
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - 재시도 대상
                    last_error = exc
                    logger.warning(
                        "handle failed order=%s attempt=%s/%s: %s",
                        event.order_id,
                        attempt,
                        self._retry.max_attempts,
                        exc,
                    )
                    if attempt < self._retry.max_attempts:
                        delay = compute_backoff_ms(attempt, self._retry)
                        await asyncio.sleep(delay / 1000)

            assert last_error is not None
            await self._handler.on_exhausted(event, last_error, attempt)
            if await self._safe_publish_dlq(payload, raw_key, last_error, attempt, tp, record.offset):
                self._tracker.mark_completed(tp, record.offset)

    async def _safe_publish_dlq(
        self,
        payload: dict | None,
        key: str | None,
        error: BaseException,
        attempts: int,
        tp: TopicPartition,
        offset: int,
    ) -> bool:
        try:
            await self._dlq.publish(
                event_payload=payload,
                key=key,
                error=error,
                attempts=attempts,
                partition=tp.partition,
                offset=offset,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            # DLQ 마저 실패하면 오프셋을 전진시키지 않는다 → 재기동 시 재처리(유실 방지).
            logger.error("DLQ publish failed, offset not committed offset=%s: %s", offset, exc)
            return False
