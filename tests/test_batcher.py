"""Tests for pipewatch.batcher."""

from datetime import datetime, timezone

import pytest

from pipewatch.batcher import Batch, Batcher
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="pipe_a", query="SELECT 1", interval=30)


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="row_count", threshold=100, comparator="gte")


def _make_result(source_name: str, healthy: bool, alert_config: AlertConfig) -> MetricResult:
    metric = PipelineMetric(source_name=source_name, value=200 if healthy else 5)
    return MetricResult(
        source_name=source_name,
        metric=metric,
        is_healthy=healthy,
        alerts=[alert_config] if not healthy else [],
        collected_at=datetime.now(timezone.utc),
    )


@pytest.fixture()
def batcher() -> Batcher:
    return Batcher(max_size=3)


def test_batcher_rejects_zero_max_size():
    with pytest.raises(ValueError):
        Batcher(max_size=0)


def test_add_returns_none_before_full(batcher, alert_config):
    r = _make_result("pipe_a", True, alert_config)
    result = batcher.add(r)
    assert result is None


def test_add_returns_batch_when_full(batcher, alert_config):
    for _ in range(2):
        batcher.add(_make_result("pipe_a", True, alert_config))
    batch = batcher.add(_make_result("pipe_a", False, alert_config))
    assert isinstance(batch, Batch)
    assert batch.size == 3


def test_batch_source_name(batcher, alert_config):
    for _ in range(3):
        batch = batcher.add(_make_result("pipe_a", True, alert_config))
    assert batch.source_name == "pipe_a"


def test_batch_healthy_and_unhealthy_counts(batcher, alert_config):
    batcher.add(_make_result("pipe_a", True, alert_config))
    batcher.add(_make_result("pipe_a", False, alert_config))
    batch = batcher.add(_make_result("pipe_a", True, alert_config))
    assert batch.healthy_count == 2
    assert batch.unhealthy_count == 1


def test_pending_count_tracks_buffer(batcher, alert_config):
    batcher.add(_make_result("pipe_a", True, alert_config))
    assert batcher.pending_count("pipe_a") == 1


def test_flush_returns_none_when_empty(batcher):
    assert batcher.flush("nonexistent") is None


def test_flush_clears_buffer(batcher, alert_config):
    batcher.add(_make_result("pipe_a", True, alert_config))
    batch = batcher.flush("pipe_a")
    assert batch is not None
    assert batch.size == 1
    assert batcher.pending_count("pipe_a") == 0


def test_flush_all_returns_all_pending(alert_config):
    b = Batcher(max_size=10)
    b.add(_make_result("pipe_a", True, alert_config))
    b.add(_make_result("pipe_b", False, alert_config))
    batches = b.flush_all()
    assert len(batches) == 2
    names = {batch.source_name for batch in batches}
    assert names == {"pipe_a", "pipe_b"}


def test_batch_summary_contains_source_name(batcher, alert_config):
    for _ in range(3):
        batch = batcher.add(_make_result("pipe_a", True, alert_config))
    assert "pipe_a" in batch.summary()


def test_buffers_are_independent_per_source(alert_config):
    b = Batcher(max_size=2)
    b.add(_make_result("pipe_a", True, alert_config))
    b.add(_make_result("pipe_b", True, alert_config))
    assert b.pending_count("pipe_a") == 1
    assert b.pending_count("pipe_b") == 1
