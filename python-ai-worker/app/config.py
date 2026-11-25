"""설정 로딩 (pydantic-settings).

우선순위: 생성자 인자 > 환경변수 > .env > config.yaml > 필드 기본값
환경변수 접두어는 ``FORTUNE_`` 이고 중첩 구분자는 ``__`` 이다.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

DEFAULT_CONFIG_FILE = os.getenv("FORTUNE_CONFIG_FILE", "config.yaml")


class ServiceSettings(BaseModel):
    name: str = "python-ai-worker"
    env: str = "local"
    log_level: str = "INFO"


class ServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


class KafkaSettings(BaseModel):
    bootstrap_servers: str = "kafka:9092"
    topic: str = "fortune.paid"
    dlq_topic: str = "fortune.paid.dlq"
    group_id: str = "fortune-ai-worker"
    auto_offset_reset: str = "earliest"
    max_concurrency: int = 8
    max_poll_records: int = 16
    poll_timeout_ms: int = 1000
    session_timeout_ms: int = 30000
    max_poll_interval_ms: int = 300000
    enable_auto_commit: bool = False


class RetrySettings(BaseModel):
    max_attempts: int = Field(default=3, ge=1)
    initial_backoff_ms: int = Field(default=200, ge=0)
    max_backoff_ms: int = Field(default=5000, ge=0)
    multiplier: float = Field(default=2.0, ge=1.0)
    jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)


class RedisSettings(BaseModel):
    url: str = "redis://redis:6379/1"
    reading_ttl_seconds: int = 604800
    cache_ttl_seconds: int = 2592000
    dedupe_ttl_seconds: int = 604800


class LlmSettings(BaseModel):
    provider: Literal["mock", "http"] = "mock"
    model: str = "fortune-mock-v1"
    timeout_seconds: float = 20.0
    temperature: float = 0.7
    endpoint: str = ""
    api_key: str = ""
    cache_enabled: bool = True


class InternalApiSettings(BaseModel):
    base_url: str = "http://spring-core:8080"
    token: str = "local-internal-token"
    timeout_seconds: float = 3.0


class FaultInjectionSettings(BaseModel):
    fail_fortune_type: str = "CRASH_TEST"


class Settings(BaseSettings):
    """애플리케이션 설정. config.yaml + 환경변수를 함께 읽는다."""

    model_config = SettingsConfigDict(
        env_prefix="FORTUNE_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        yaml_file=DEFAULT_CONFIG_FILE,
    )

    service: ServiceSettings = ServiceSettings()
    server: ServerSettings = ServerSettings()
    kafka: KafkaSettings = KafkaSettings()
    retry: RetrySettings = RetrySettings()
    redis: RedisSettings = RedisSettings()
    llm: LlmSettings = LlmSettings()
    internal_api: InternalApiSettings = InternalApiSettings()
    fault_injection: FaultInjectionSettings = FaultInjectionSettings()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # 앞에 올수록 우선순위가 높다 → 환경변수가 config.yaml 을 덮어쓴다.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """테스트에서 설정을 다시 읽을 때 사용."""
    get_settings.cache_clear()
