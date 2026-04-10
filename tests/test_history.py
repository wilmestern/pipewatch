"""Tests for pipewatch.history module."""

import pytest

from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.history import HistoryStore, SourceHistory, MetricSnapshot


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def healthy_result():
    metric = PipelineMetric(row_count=100, error_rate=0.0, latency_seconds=1.0)
    return MetricResult(source="src", metric=metric, healthy=True, message="ok")


@pytest.fixture
def unhealthy_result():
    metric = PipelineMetric(row_count=0, error_rate=0.9, latency_seconds=30.0)
    return MetricResult(source="src", metric=metric, healthy=False, message="high error rate")


@pytest.fixture
def store():
    return HistoryStore(max_entries=5)


# ---------------------------------------------------------------------------
# SourceHistory tests
# ---------------------------------------------------------------------------

def test_latest_returns_none_when_empty():
    history = SourceHistory(source_name="src")
    assert history.latest() is None


def test_record_stores_snapshot(healthy_result):
    history = SourceHistory(source_name="src")
    history.record(healthy_result)
    assert history.latest() is not None
    assert history.latest().result is healthy_result


def test_max_entries_respected(healthy_result):
    history = SourceHistory(source_name="src", max_entries=3)
    for _ in range(5):
        history.record(healthy_result)
    assert len(history.all()) == 3


def test_failure_rate_all_healthy(healthy_result):
    history = SourceHistory(source_name="src")
    for _ in range(4):
        history.record(healthy_result)
    assert history.failure_rate() == 0.0


def test_failure_rate_mixed(healthy_result, unhealthy_result):
    history = SourceHistory(source_name="src")
    history.record(healthy_result)
    history.record(unhealthy_result)
    assert history.failure_rate() == pytest.approx(0.5)


def test_failure_rate_empty():
    history = SourceHistory(source_name="src")
    assert history.failure_rate() == 0.0


def test_consecutive_failures_none(healthy_result):
    history = SourceHistory(source_name="src")
    history.record(healthy_result)
    assert history.consecutive_failures() == 0


def test_consecutive_failures_count(healthy_result, unhealthy_result):
    history = SourceHistory(source_name="src")
    history.record(healthy_result)
    history.record(unhealthy_result)
    history.record(unhealthy_result)
    assert history.consecutive_failures() == 2


# ---------------------------------------------------------------------------
# HistoryStore tests
# ---------------------------------------------------------------------------

def test_store_record_and_get(store, healthy_result):
    store.record("pipe_a", healthy_result)
    history = store.get("pipe_a")
    assert history is not None
    assert history.latest().result is healthy_result


def test_store_get_unknown_source(store):
    assert store.get("nonexistent") is None


def test_store_sources(store, healthy_result, unhealthy_result):
    store.record("pipe_a", healthy_result)
    store.record("pipe_b", unhealthy_result)
    assert set(store.sources()) == {"pipe_a", "pipe_b"}


def test_store_respects_max_entries(store, healthy_result):
    for _ in range(10):
        store.record("pipe_a", healthy_result)
    assert len(store.get("pipe_a").all()) == 5
