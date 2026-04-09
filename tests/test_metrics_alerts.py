"""Tests for pipewatch.metrics and pipewatch.alerts."""

import pytest
from datetime import datetime

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricsCollector, PipelineMetric
from pipewatch.alerts import Alert, AlertEvaluator


@pytest.fixture
def source_config():
    return SourceConfig(name="test_source", min_threshold=10.0, max_threshold=90.0)


@pytest.fixture
def alert_config():
    return AlertConfig(min_threshold=10.0, max_threshold=90.0, severity="critical")


@pytest.fixture
def collector(source_config):
    return MetricsCollector(source_config)


@pytest.fixture
def evaluator(alert_config):
    return AlertEvaluator(alert_config)


# --- MetricsCollector tests ---

def test_record_returns_success(collector):
    result = collector.record(50.0)
    assert result.success is True
    assert result.metric is not None
    assert result.metric.value == 50.0


def test_latest_returns_most_recent(collector):
    collector.record(20.0)
    collector.record(55.0)
    assert collector.latest().value == 55.0


def test_latest_returns_none_when_empty(collector):
    assert collector.latest() is None


def test_history_respects_limit(collector):
    for i in range(10):
        collector.record(float(i))
    assert len(collector.history(limit=5)) == 5


def test_clear_history(collector):
    collector.record(42.0)
    collector.clear_history()
    assert collector.history() == []


def test_metric_is_healthy_within_bounds(source_config):
    metric = PipelineMetric(source_name="s", value=50.0)
    assert metric.is_healthy(source_config) is True


def test_metric_is_unhealthy_below_min(source_config):
    metric = PipelineMetric(source_name="s", value=5.0)
    assert metric.is_healthy(source_config) is False


def test_metric_is_unhealthy_above_max(source_config):
    metric = PipelineMetric(source_name="s", value=95.0)
    assert metric.is_healthy(source_config) is False


# --- AlertEvaluator tests ---

def test_no_alert_within_bounds(evaluator):
    metric = PipelineMetric(source_name="test_source", value=50.0)
    assert evaluator.evaluate(metric) is None


def test_alert_triggered_below_min(evaluator):
    metric = PipelineMetric(source_name="test_source", value=5.0)
    alert = evaluator.evaluate(metric)
    assert alert is not None
    assert alert.severity == "critical"
    assert "below minimum" in alert.message


def test_alert_triggered_above_max(evaluator):
    metric = PipelineMetric(source_name="test_source", value=99.0)
    alert = evaluator.evaluate(metric)
    assert alert is not None
    assert "above maximum" in alert.message


def test_active_alerts_list(evaluator):
    metric = PipelineMetric(source_name="test_source", value=1.0)
    evaluator.evaluate(metric)
    assert len(evaluator.active_alerts()) == 1


def test_resolve_all_clears_active(evaluator):
    metric = PipelineMetric(source_name="test_source", value=1.0)
    evaluator.evaluate(metric)
    evaluator.resolve_all()
    assert evaluator.active_alerts() == []
