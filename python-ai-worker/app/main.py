"""FastAPI 진입점.

- 앱 기동 시 Kafka 컨슈머를 백그라운드 태스크로 시작한다(API 와 워커가 같은 프로세스).
- ``GET /api/v1/readings/{orderId}`` 는 Redis 만 읽는다 → AI 워커가 바빠도 조회는 즉시 응답.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app.config import Settings, get_settings
from app.dlq import DlqPublisher, create_dlq_producer
from app.handler import ReadingHandler
from app.kafka_worker import FortuneConsumer
from app.llm import build_llm_client
from app.redis_store import RedisReadingStore, create_redis
from app.schemas import ReadingResponse, ReadingStatus
from app.clients.internal_api import InternalApiClient

logger = logging.getLogger(__name__)


def configure_logging(settings: Settings) -> None:
    logging.basicConfig(
        level=getattr(logging, settings.service.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings)
    logger.info("starting %s (env=%s)", settings.service.name, settings.service.env)

    redis = create_redis(settings.redis)
    store = RedisReadingStore(redis, settings.redis)
    internal_api = InternalApiClient(settings.internal_api)
    dlq_producer = create_dlq_producer(settings.kafka)
    await dlq_producer.start()

    handler = ReadingHandler(
        settings=settings,
        store=store,
        llm=build_llm_client(settings.llm),
        internal_api=internal_api,
    )
    dlq = DlqPublisher(dlq_producer, settings.kafka, settings.kafka.group_id)
    consumer = FortuneConsumer(settings.kafka, settings.retry, handler, dlq)

    app.state.settings = settings
    app.state.store = store
    app.state.consumer = consumer
    app.state.internal_api = internal_api
    app.state.dlq_producer = dlq_producer

    try:
        await consumer.start()
    except Exception as exc:  # noqa: BLE001 - Kafka 가 늦게 뜨면 API 는 살리고 재시도
        logger.error("consumer failed to start (API still serving): %s", exc)

    try:
        yield
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        await internal_api.close()
        await store.close()
        logger.info("shutdown complete")


app = FastAPI(title="fortune python-ai-worker", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    checks: dict[str, str] = {}
    store = getattr(request.app.state, "store", None)
    try:
        await store._redis.ping()  # noqa: SLF001 - 준비 상태 확인용 단일 호출
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"
    healthy = checks["redis"] == "ok"
    return JSONResponse(status_code=200 if healthy else 503, content={"checks": checks})


@app.get("/api/v1/readings/{order_id}", response_model=ReadingResponse)
async def get_reading(order_id: str, request: Request) -> ReadingResponse:
    store = request.app.state.store
    status_record = await store.get_status(order_id)
    if status_record is None:
        raise HTTPException(status_code=404, detail={"code": "READING_NOT_FOUND", "orderId": order_id})

    result = None
    if status_record.status is ReadingStatus.COMPLETED:
        result = await store.get_result(order_id)

    return ReadingResponse(
        order_id=order_id,
        status=status_record.status,
        attempts=status_record.attempts,
        message=status_record.message,
        updated_at=status_record.updated_at,
        result=result,
    )
