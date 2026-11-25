"""DLQ 발행기.

fortune.paid.dlq 로 원본 이벤트 + 오류 정보를 envelope 로 감싸 보낸다.
헤더에도 원본 위치를 남겨 두어 컨슈머/운영 도구가 바로 추적할 수 있게 한다.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from app.config import KafkaSettings
from app.schemas import DlqEnvelope

logger = logging.getLogger(__name__)


class DlqPublisher:
    def __init__(self, producer: AIOKafkaProducer, settings: KafkaSettings, group_id: str) -> None:
        self._producer = producer
        self._settings = settings
        self._group_id = group_id

    async def publish(
        self,
        *,
        event_payload: dict | None,
        key: str | None,
        error: BaseException,
        attempts: int,
        partition: int | None = None,
        offset: int | None = None,
    ) -> None:
        envelope = DlqEnvelope(
            originalTopic=self._settings.topic,
            originalPartition=partition,
            originalOffset=offset,
            groupId=self._group_id,
            errorType=type(error).__name__,
            errorMessage=str(error)[:1000],
            attempts=attempts,
            failedAt=datetime.now(timezone.utc),
            event=event_payload,
        )
        headers = [
            ("x-error", str(type(error).__name__).encode()),
            ("x-attempts", str(attempts).encode()),
            ("x-original-topic", self._settings.topic.encode()),
            ("x-original-partition", str(partition).encode() if partition is not None else b""),
            ("x-original-offset", str(offset).encode() if offset is not None else b""),
        ]
        await self._producer.send_and_wait(
            self._settings.dlq_topic,
            key=(key.encode() if key else None),
            value=json.dumps(envelope.model_dump(by_alias=True), ensure_ascii=False).encode("utf-8"),
            headers=headers,
        )
        logger.error(
            "published to DLQ topic=%s order_key=%s attempts=%s error=%s",
            self._settings.dlq_topic,
            key,
            attempts,
            error,
        )


def create_dlq_producer(settings: KafkaSettings) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        compression_type="lz4",
        linger_ms=5,
        client_id="fortune-ai-worker-dlq",
    )
