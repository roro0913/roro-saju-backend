"""핸들러 동작 테스트 (메모리 저장소 + 목 LLM)."""

import pytest

from app.config import get_settings
from app.handler import ReadingFailure, ReadingHandler
from app.llm import MockLlmClient
from app.redis_store import InMemoryReadingStore
from app.schemas import FortunePaidEvent, ReadingStatus
from app.clients.internal_api import InMemoryInternalApiClient


def make_event(order_id: str = "order-1", event_id: str = "evt-1", fortune_type: str = "TODAY") -> FortunePaidEvent:
    return FortunePaidEvent.model_validate(
        {
            "eventId": event_id,
            "eventType": "fortune.paid",
            "orderId": order_id,
            "paymentId": "pay-1",
            "userId": "u-1001",
            "amount": 4900,
            "currency": "KRW",
            "fortuneType": fortune_type,
            "readingInput": {
                "birthDate": "1993-09-13",
                "birthTime": "03:53",
                "calendar": "SOLAR",
                "gender": "M",
            },
        }
    )


def build(store=None, internal=None) -> tuple[ReadingHandler, InMemoryReadingStore, InMemoryInternalApiClient]:
    settings = get_settings()
    store = store or InMemoryReadingStore()
    internal = internal or InMemoryInternalApiClient()
    handler = ReadingHandler(settings, store, MockLlmClient("test-model"), internal)
    return handler, store, internal


async def test_handler_completes_and_notifies_spring() -> None:
    handler, store, internal = build()
    result = await handler.handle(make_event())

    assert result.status is ReadingStatus.COMPLETED
    assert result.cache_hit is False
    status = await store.get_status("order-1")
    assert status is not None and status.status is ReadingStatus.COMPLETED
    reading = await store.get_result("order-1")
    assert reading is not None and reading.order_id == "order-1" and reading.body
    assert internal.calls == [("order-1", "COMPLETED", None)]


async def test_second_order_with_same_input_hits_cache() -> None:
    handler, store, _ = build()
    await handler.handle(make_event(order_id="order-1", event_id="evt-1"))
    second = await handler.handle(make_event(order_id="order-2", event_id="evt-2"))

    assert second.cache_hit is True
    cached = await store.get_result("order-2")
    assert cached is not None and cached.cached is True


async def test_duplicate_event_is_skipped() -> None:
    handler, _, internal = build()
    await handler.handle(make_event(event_id="evt-dup"))
    again = await handler.handle(make_event(event_id="evt-dup"))

    assert again.skipped is True
    assert len(internal.calls) == 1


async def test_fault_injection_raises_for_dlq_verification() -> None:
    handler, store, _ = build()
    with pytest.raises(ReadingFailure):
        await handler.handle(make_event(fortune_type="CRASH_TEST"))

    status = await store.get_status("order-1")
    assert status is not None and status.status is ReadingStatus.PROCESSING


async def test_on_exhausted_releases_dedupe_key_for_dlq_replay() -> None:
    """DLQ 에서 원인을 고친 뒤 재주입하면 다시 처리되어야 한다(중복 방어 키 해제)."""
    handler, store, _ = build()
    await handler.on_exhausted(make_event(event_id="evt-retry"), ReadingFailure("boom"), attempts=3)

    retried = await handler.handle(make_event(event_id="evt-retry"))
    assert retried.skipped is False


async def test_on_exhausted_marks_failed() -> None:
    handler, store, internal = build()
    await handler.on_exhausted(make_event(), ReadingFailure("boom"), attempts=3)

    status = await store.get_status("order-1")
    assert status is not None and status.status is ReadingStatus.FAILED and status.attempts == 3
    assert internal.calls[0][1] == "FAILED"
