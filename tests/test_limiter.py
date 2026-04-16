"""Tests for pipewatch.limiter."""
import pytest
from pipewatch.limiter import Limiter, LimiterCapacityError


@pytest.fixture
def limiter() -> Limiter:
    return Limiter(max_slots=2)


def test_rejects_zero_max_slots():
    with pytest.raises(ValueError):
        Limiter(max_slots=0)


def test_initial_available_slots(limiter):
    assert limiter.available_slots == 2


def test_acquire_reduces_available_slots(limiter):
    limiter.acquire("source_a")
    assert limiter.available_slots == 1


def test_acquire_returns_slot_usage(limiter):
    usage = limiter.acquire("source_a")
    assert usage.source_name == "source_a"
    assert usage.active is True
    assert usage.acquired_at is not None


def test_is_active_after_acquire(limiter):
    limiter.acquire("source_a")
    assert limiter.is_active("source_a") is True


def test_is_not_active_before_acquire(limiter):
    assert limiter.is_active("source_a") is False


def test_release_frees_slot(limiter):
    limiter.acquire("source_a")
    limiter.release("source_a")
    assert limiter.available_slots == 2


def test_release_marks_usage_inactive(limiter):
    limiter.acquire("source_a")
    usage = limiter.release("source_a")
    assert usage is not None
    assert usage.active is False
    assert usage.released_at is not None


def test_release_returns_none_for_unknown_source(limiter):
    result = limiter.release("nonexistent")
    assert result is None


def test_capacity_error_when_full(limiter):
    limiter.acquire("source_a")
    limiter.acquire("source_b")
    with pytest.raises(LimiterCapacityError):
        limiter.acquire("source_c")


def test_active_count_reflects_held_slots(limiter):
    limiter.acquire("source_a")
    assert limiter.active_count == 1


def test_history_records_released_slots(limiter):
    limiter.acquire("source_a")
    limiter.release("source_a")
    history = limiter.history()
    assert len(history) == 1
    assert history[0].source_name == "source_a"


def test_history_does_not_include_active_slots(limiter):
    limiter.acquire("source_a")
    assert limiter.history() == []


def test_duration_seconds_set_after_release(limiter):
    limiter.acquire("source_a")
    usage = limiter.release("source_a")
    assert usage.duration_seconds is not None
    assert usage.duration_seconds >= 0.0
