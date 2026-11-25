"""재시도 백오프 / 오프셋 커밋 트래커 테스트."""

from aiokafka import TopicPartition

from app.config import RetrySettings
from app.kafka_worker import OffsetTracker, compute_backoff_ms

SETTINGS = RetrySettings(
    max_attempts=3, initial_backoff_ms=200, max_backoff_ms=5000, multiplier=2.0, jitter_ratio=0.2
)


def test_backoff_grows_exponentially_without_jitter() -> None:
    no_jitter = SETTINGS.model_copy(update={"jitter_ratio": 0.0})
    assert compute_backoff_ms(1, no_jitter) == 200
    assert compute_backoff_ms(2, no_jitter) == 400
    assert compute_backoff_ms(3, no_jitter) == 800


def test_backoff_is_capped() -> None:
    no_jitter = SETTINGS.model_copy(update={"jitter_ratio": 0.0})
    assert compute_backoff_ms(10, no_jitter) == 5000


def test_backoff_jitter_stays_within_ratio() -> None:
    low = compute_backoff_ms(2, SETTINGS, rand=lambda: 0.0)
    high = compute_backoff_ms(2, SETTINGS, rand=lambda: 1.0)
    assert 320 <= low <= 400
    assert 400 <= high <= 480
    assert low <= high


def test_offset_tracker_commits_only_contiguous_range() -> None:
    tp = TopicPartition("fortune.paid", 0)
    tracker = OffsetTracker()

    tracker.mark_completed(tp, 10)
    assert tracker.drain()[tp].offset == 11

    # 11 이 아직 처리 중이면 12, 13 이 끝나도 커밋은 11 까지만
    tracker.mark_completed(tp, 12)
    tracker.mark_completed(tp, 13)
    assert tracker.drain() == {}

    tracker.mark_completed(tp, 11)
    assert tracker.drain()[tp].offset == 14
    assert tracker.drain() == {}


def test_offset_tracker_handles_partitions_independently() -> None:
    p0 = TopicPartition("fortune.paid", 0)
    p1 = TopicPartition("fortune.paid", 1)
    tracker = OffsetTracker()
    tracker.mark_completed(p0, 5)
    tracker.mark_completed(p1, 100)
    commits = tracker.drain()
    assert commits[p0].offset == 6
    assert commits[p1].offset == 101
