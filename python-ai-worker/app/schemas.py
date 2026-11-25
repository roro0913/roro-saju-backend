"""이벤트/응답 스키마.

- Kafka 이벤트와 Redis JSON 은 **camelCase**(spring-core 와 동일) 를 쓴다.
- FastAPI 응답도 alias 로 직렬화해 앱 클라이언트가 camelCase 로 받는다.
- 입력은 camelCase / snake_case 둘 다 허용한다(populate_by_name).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

CAMEL = ConfigDict(extra="ignore", populate_by_name=True)


class ReadingStatus(str, Enum):
    """처리 상태. spring-core 가 기록하는 문자열과 동일하다."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ReadingInput(BaseModel):
    model_config = CAMEL

    birth_date: str = Field(alias="birthDate")
    birth_time: str | None = Field(default=None, alias="birthTime")
    calendar: Literal["SOLAR", "LUNAR"] = "SOLAR"
    gender: Literal["M", "F", "U"] = "U"
    question: str | None = None


class FortunePaidEvent(BaseModel):
    """결제 확정 이벤트 (topic: fortune.paid, key: orderId)."""

    model_config = CAMEL

    event_id: str = Field(alias="eventId")
    event_type: str = Field(default="fortune.paid", alias="eventType")
    occurred_at: str | None = Field(default=None, alias="occurredAt")
    order_id: str = Field(alias="orderId")
    payment_id: str | None = Field(default=None, alias="paymentId")
    user_id: str | None = Field(default=None, alias="userId")
    amount: Decimal | None = None  # NUMERIC(12,2) 는 4900.00 처럼 소수점을 달고 온다
    currency: str | None = "KRW"
    fortune_type: str = Field(default="TODAY", alias="fortuneType")
    # 필수 필드로 선언한다: 없으면 재시도 없이 즉시 DLQ 로 격리하는 편이 맞다(스키마 위반은 재시도 무의미).
    reading_input: ReadingInput = Field(alias="readingInput")

    def normalised_input(self) -> dict[str, Any]:
        """캐시 키 계산용: None 필드를 제거해 같은 의미의 입력이 같은 해시가 되게 한다."""
        data = self.reading_input.model_dump(by_alias=True)
        return {k: v for k, v in data.items() if v is not None}


class ReadingResult(BaseModel):
    model_config = CAMEL

    order_id: str = Field(alias="orderId")
    headline: str
    body: str
    model: str
    cached: bool = False
    generated_at: datetime = Field(alias="generatedAt")


class ReadingStatusRecord(BaseModel):
    """Redis ``reading:status:{orderId}`` 값 (spring-core 와 python 양쪽이 쓴다)."""

    model_config = CAMEL

    order_id: str = Field(alias="orderId")
    status: ReadingStatus
    attempts: int = 0
    message: str | None = None
    updated_at: datetime = Field(alias="updatedAt")


class ReadingResponse(BaseModel):
    """GET /api/v1/readings/{orderId} 응답."""

    model_config = CAMEL

    order_id: str = Field(alias="orderId")
    status: ReadingStatus
    attempts: int = 0
    message: str | None = None
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    result: ReadingResult | None = None


class DlqEnvelope(BaseModel):
    """fortune.paid.dlq 메시지 본문."""

    model_config = CAMEL

    original_topic: str = Field(alias="originalTopic")
    original_partition: int | None = Field(default=None, alias="originalPartition")
    original_offset: int | None = Field(default=None, alias="originalOffset")
    group_id: str | None = Field(default=None, alias="groupId")
    error_type: str = Field(alias="errorType")
    error_message: str = Field(alias="errorMessage")
    attempts: int = 0
    failed_at: datetime = Field(alias="failedAt")
    event: dict[str, Any] | None = None
