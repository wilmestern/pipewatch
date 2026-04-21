"""Tests for pipewatch.windower."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.history import MetricSnapshot, SourceHistory
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.windower import WindowStats, Windower


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="db", type="postgres", connection="postgresql://localhost/test")


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="row_count", metric="row_count", threshold=10.0, operator="gte")


def _make_snapshot(source_config, alert_config, value: float, healthy: bool, age_seconds: float) -> MetricSnapshot:
    metric = PipelineMetric(name="row_count", value=value)
    result = MetricResult(source=source_config, metric=metric, is_healthy=healthy, alerts=[])
    ts = datetime.utcnow() - timedelta(seconds=age_seconds)
    return MetricSnapshot(source_name=source_config.name, result=result, recorded_at=ts)


@pytest.fixture()
def store(source_config, alert_config) -> SourceHistory:
    s = SourceHistory()
    for age, val, ok in [(5, 20.0, True), (15, 5.0, False), (25, 18.0, True)]:
        snap = _make_snapshot(source_config, alert_config, val, ok, age)
        s._records.setdefault(source_config.name, []).append(snap)
    return s


@pytest.fixture()
def windower(store) -> Windower:
    return Windower(store)


def test_compute_returns_window_stats(windower, source_config):
    result = windower.compute(source_config.name, window_seconds=60)
    assert isinstance(result, WindowStats)


def test_sample_count_within_window(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=60)
    assert stats.sample_count == 3


def test_sample_count_narrow_window(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=10)
    assert stats.sample_count == 1


def test_healthy_count(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=60)
    assert stats.healthy_count == 2
    assert stats.unhealthy_count == 1


def test_health_rate(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=60)
    assert abs(stats.health_rate - 2 / 3) < 1e-9


def test_avg_value(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=60)
    assert stats.avg_value == pytest.approx((20.0 + 5.0 + 18.0) / 3)


def test_min_max_value(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=60)
    assert stats.min_value == pytest.approx(5.0)
    assert stats.max_value == pytest.approx(20.0)


def test_empty_window_returns_zero_samples(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=1)
    assert stats.sample_count == 0
    assert stats.avg_value is None
    assert stats.health_rate == 1.0


def test_unknown_source_returns_empty(windower):
    stats = windower.compute("nonexistent", window_seconds=60)
    assert stats.sample_count == 0


def test_summary_contains_source_name(windower, source_config):
    stats = windower.compute(source_config.name, window_seconds=60)
    assert source_config.name in stats.summary()
