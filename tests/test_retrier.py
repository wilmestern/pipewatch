"""Tests for pipewatch.retrier."""

from __future__ import annotations

import pytest

from pipewatch.retrier import RetryPolicy, RetryResult, Retrier


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------


def test_delay_for_first_attempt_is_zero():
    policy = RetryPolicy(delay_seconds=2.0, backoff_factor=2.0)
    assert policy.delay_for(0) == 0.0


def test_delay_for_second_attempt_equals_base_delay():
    policy = RetryPolicy(delay_seconds=2.0, backoff_factor=2.0)
    assert policy.delay_for(1) == 2.0


def test_delay_for_third_attempt_doubles():
    policy = RetryPolicy(delay_seconds=2.0, backoff_factor=2.0)
    assert policy.delay_for(2) == 4.0


def test_delay_capped_at_max():
    policy = RetryPolicy(delay_seconds=5.0, backoff_factor=10.0, max_delay_seconds=15.0)
    assert policy.delay_for(3) == 15.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fast_policy():
    """Zero-delay policy so tests don't actually sleep."""
    return RetryPolicy(max_attempts=3, delay_seconds=0.0, backoff_factor=1.0)


@pytest.fixture()
def retrier(fast_policy):
    return Retrier(policy=fast_policy)


# ---------------------------------------------------------------------------
# Retrier
# ---------------------------------------------------------------------------


def test_success_on_first_attempt(retrier):
    result = retrier.run(lambda: 42, source_name="src")
    assert result.success is True
    assert result.attempts == 1
    assert result.value == 42
    assert result.last_exception is None


def test_success_after_transient_failure(fast_policy):
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("transient")
        return "ok"

    retrier = Retrier(policy=fast_policy)
    result = retrier.run(flaky, source_name="flaky-src")
    assert result.success is True
    assert result.attempts == 2
    assert result.value == "ok"


def test_failure_after_all_attempts_exhausted(fast_policy):
    def always_fails():
        raise RuntimeError("permanent")

    retrier = Retrier(policy=fast_policy)
    result = retrier.run(always_fails, source_name="bad-src")
    assert result.success is False
    assert result.attempts == fast_policy.max_attempts
    assert isinstance(result.last_exception, RuntimeError)


def test_summary_on_success(retrier):
    result = retrier.run(lambda: None)
    assert "succeeded" in result.summary


def test_summary_on_failure(fast_policy):
    retrier = Retrier(policy=fast_policy)
    result = retrier.run(lambda: (_ for _ in ()).throw(IOError("boom")))
    assert "failed" in result.summary
    assert "boom" in result.summary


def test_default_policy_used_when_none_provided():
    r = Retrier()
    assert r.policy.max_attempts == 3


def test_retry_result_stores_value_on_success(retrier):
    result = retrier.run(lambda: {"key": "val"})
    assert result.value == {"key": "val"}
