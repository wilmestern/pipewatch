"""Tests for pipewatch.comparator."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.history import SourceHistory
from pipewatch.comparator import Comparator, ComparisonResult


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="db", source_type="postgres", connection_string="postgresql://localhost/test")


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="latency", metric="latency_ms", threshold=200.0, operator="gt")


def _make_result(cfg: SourceConfig, acfg: AlertConfig, value: float) -> MetricResult:
    metric = PipelineMetric(source_name=cfg.name, metric_name=acfg.metric, value=value, timestamp=datetime.now(timezone.utc))
    return MetricResult(source_config=cfg, metric=metric, is_healthy=value <= acfg.threshold)


@pytest.fixture()
def store(source_config: SourceConfig, alert_config: AlertConfig) -> SourceHistory:
    h = SourceHistory()
    for v in [100.0, 110.0, 90.0, 105.0, 95.0]:
        h.record(source_config.name, _make_result(source_config, alert_config, v))
    return h


@pytest.fixture()
def comparator(store: SourceHistory) -> Comparator:
    return Comparator(history=store, window_size=4)


def test_compare_returns_none_when_insufficient_data(source_config, alert_config):
    h = SourceHistory()
    h.record(source_config.name, _make_result(source_config, alert_config, 100.0))
    c = Comparator(history=h, window_size=4)
    assert c.compare(source_config.name) is None


def test_compare_returns_comparison_result(comparator, source_config):
    result = comparator.compare(source_config.name)
    assert isinstance(result, ComparisonResult)


def test_compare_current_value_is_latest(comparator, source_config):
    result = comparator.compare(source_config.name)
    assert result.current_value == pytest.approx(95.0)


def test_compare_baseline_is_average_of_window(comparator, source_config):
    result = comparator.compare(source_config.name)
    expected_baseline = (100.0 + 110.0 + 90.0 + 105.0) / 4
    assert result.baseline_value == pytest.approx(expected_baseline)


def test_compare_delta_is_current_minus_baseline(comparator, source_config):
    result = comparator.compare(source_config.name)
    assert result.delta == pytest.approx(result.current_value - result.baseline_value)


def test_compare_improved_when_current_lower(comparator, source_config):
    result = comparator.compare(source_config.name)
    assert result.improved is True


def test_compare_degraded_when_current_higher(source_config, alert_config):
    h = SourceHistory()
    for v in [90.0, 90.0, 90.0, 90.0, 200.0]:
        h.record(source_config.name, _make_result(source_config, alert_config, v))
    c = Comparator(history=h, window_size=4)
    result = c.compare(source_config.name)
    assert result.improved is False


def test_delta_pct_none_when_baseline_is_zero(source_config, alert_config):
    h = SourceHistory()
    for v in [0.0, 0.0, 0.0, 0.0, 10.0]:
        h.record(source_config.name, _make_result(source_config, alert_config, v))
    c = Comparator(history=h, window_size=4)
    result = c.compare(source_config.name)
    assert result.delta_pct is None


def test_compare_all_returns_list(comparator):
    results = comparator.compare_all()
    assert isinstance(results, list)
    assert len(results) == 1


def test_window_size_validation():
    with pytest.raises(ValueError):
        Comparator(history=SourceHistory(), window_size=0)


def test_summary_contains_source_name(comparator, source_config):
    result = comparator.compare(source_config.name)
    assert source_config.name in result.summary()
