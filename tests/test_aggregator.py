"""Tests for pipewatch.aggregator."""

import pytest

from pipewatch.config import SourceConfig
from pipewatch.history import SourceHistory
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.aggregator import Aggregator, AggregateStats


@pytest.fixture()
def source_config():
    return SourceConfig(name="db", type="postgres", connection_string="postgresql://localhost/test")


def _make_result(source_config, success=True, row_count=100, latency_ms=50.0):
    metric = PipelineMetric(
        source_name=source_config.name,
        success=success,
        row_count=row_count,
        latency_ms=latency_ms,
    )
    return MetricResult(source=source_config, metric=metric, error=None)


@pytest.fixture()
def store(source_config):
    h = SourceHistory()
    for _ in range(3):
        h.record(source_config.name, _make_result(source_config, success=True, row_count=200, latency_ms=40.0))
    h.record(source_config.name, _make_result(source_config, success=False, row_count=None, latency_ms=None))
    return h


@pytest.fixture()
def aggregator(store):
    return Aggregator(store)


def test_compute_returns_aggregate_stats(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert isinstance(stats, AggregateStats)


def test_compute_sample_count(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert stats.sample_count == 4


def test_compute_success_rate(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert stats.success_rate == pytest.approx(0.75)


def test_compute_avg_row_count(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    # Only 3 snapshots have row_count=200; the failed one has None
    assert stats.avg_row_count == pytest.approx(200.0)


def test_compute_avg_latency_ms(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert stats.avg_latency_ms == pytest.approx(40.0)


def test_compute_min_max_row_count(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert stats.min_row_count == 200
    assert stats.max_row_count == 200


def test_compute_empty_source_returns_zero_stats(source_config):
    empty_store = SourceHistory()
    agg = Aggregator(empty_store)
    stats = agg.compute(source_config.name)
    assert stats.sample_count == 0
    assert stats.success_rate == 0.0
    assert stats.avg_row_count is None


def test_compute_last_n_limits_samples(aggregator, source_config):
    # last 2 snapshots: index 2 (success) and 3 (failure) → 50 % success
    stats = aggregator.compute(source_config.name, last_n=2)
    assert stats.sample_count == 2
    assert stats.success_rate == pytest.approx(0.5)


def test_is_healthy_true_when_all_succeed(source_config):
    h = SourceHistory()
    for _ in range(3):
        h.record(source_config.name, _make_result(source_config, success=True))
    stats = Aggregator(h).compute(source_config.name)
    assert stats.is_healthy is True


def test_is_healthy_false_when_any_fail(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert stats.is_healthy is False


def test_summary_contains_source_name(aggregator, source_config):
    stats = aggregator.compute(source_config.name)
    assert source_config.name in stats.summary()
