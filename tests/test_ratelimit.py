"""Tests for pipewatch.ratelimit module."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.ratelimit import RateLimitRule, RateLimitState, RateLimiter


@pytest.fixture
def rule() -> RateLimitRule:
    return RateLimitRule(source_name="db_pipeline", max_calls=3, window_seconds=60)


@pytest.fixture
def limiter(rule: RateLimitRule) -> RateLimiter:
    return RateLimiter(rules=[rule])


def test_not_rate_limited_before_any_calls(limiter: RateLimiter) -> None:
    assert limiter.is_rate_limited("db_pipeline") is False


def test_not_rate_limited_for_unknown_source(limiter: RateLimiter) -> None:
    assert limiter.is_rate_limited("unknown_source") is False


def test_rate_limited_after_max_calls_reached(limiter: RateLimiter) -> None:
    for _ in range(3):
        limiter.record_call("db_pipeline")
    assert limiter.is_rate_limited("db_pipeline") is True


def test_not_rate_limited_below_max_calls(limiter: RateLimiter) -> None:
    for _ in range(2):
        limiter.record_call("db_pipeline")
    assert limiter.is_rate_limited("db_pipeline") is False


def test_rate_limit_resets_after_window(limiter: RateLimiter) -> None:
    past = datetime.utcnow() - timedelta(seconds=120)
    limiter._states["db_pipeline"].call_times = [past, past, past]
    assert limiter.is_rate_limited("db_pipeline") is False


def test_remaining_calls_returns_none_for_unknown_source(limiter: RateLimiter) -> None:
    assert limiter.remaining_calls("no_rule_source") is None


def test_remaining_calls_full_when_no_calls_made(limiter: RateLimiter) -> None:
    assert limiter.remaining_calls("db_pipeline") == 3


def test_remaining_calls_decrements_on_record(limiter: RateLimiter) -> None:
    limiter.record_call("db_pipeline")
    limiter.record_call("db_pipeline")
    assert limiter.remaining_calls("db_pipeline") == 1


def test_remaining_calls_zero_when_exhausted(limiter: RateLimiter) -> None:
    for _ in range(3):
        limiter.record_call("db_pipeline")
    assert limiter.remaining_calls("db_pipeline") == 0


def test_record_call_for_source_without_rule() -> None:
    limiter = RateLimiter()
    limiter.record_call("unregistered")
    assert limiter.is_rate_limited("unregistered") is False


def test_add_rule_after_init() -> None:
    limiter = RateLimiter()
    new_rule = RateLimitRule(source_name="kafka", max_calls=5, window_seconds=30)
    limiter.add_rule(new_rule)
    for _ in range(5):
        limiter.record_call("kafka")
    assert limiter.is_rate_limited("kafka") is True


def test_prune_removes_old_entries() -> None:
    state = RateLimitState()
    old_time = datetime.utcnow() - timedelta(seconds=200)
    state.call_times = [old_time, old_time]
    state.prune(timedelta(seconds=60))
    assert len(state.call_times) == 0
