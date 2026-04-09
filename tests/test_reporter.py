"""Tests for the Reporter and PipelineReport classes."""

from datetime import datetime

import pytest

from pipewatch.alerts import Alert
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.reporter import PipelineReport, Reporter


@pytest.fixture
def source_config():
    return SourceConfig(name="orders-db", type="postgres", connection="postgresql://localhost/orders")


@pytest.fixture
def alert_config():
    return AlertConfig(name="high-latency", metric="latency", threshold=500.0, operator="gt")


@pytest.fixture
def healthy_metric():
    return PipelineMetric(name="latency", value=120.0, source="orders-db", timestamp=datetime(2024, 1, 1, 12, 0, 0))


@pytest.fixture
def unhealthy_metric():
    return PipelineMetric(name="latency", value=800.0, source="orders-db", timestamp=datetime(2024, 1, 1, 12, 0, 0))


@pytest.fixture
def reporter():
    return Reporter(use_color=False)


def test_pipeline_report_is_healthy_when_no_alerts(healthy_metric):
    result = MetricResult(metric=healthy_metric, healthy=True)
    report = PipelineReport(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        source_name="orders-db",
        metric_results=[result],
        active_alerts=[],
    )
    assert report.is_healthy is True
    assert report.status_label == "HEALTHY"


def test_pipeline_report_is_degraded_with_alerts(unhealthy_metric, alert_config):
    result = MetricResult(metric=unhealthy_metric, healthy=False)
    alert = Alert(alert_config=alert_config, message="latency exceeded threshold", triggered_at=datetime(2024, 1, 1, 12, 0, 0))
    report = PipelineReport(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        source_name="orders-db",
        metric_results=[result],
        active_alerts=[alert],
    )
    assert report.is_healthy is False
    assert report.status_label == "DEGRADED"


def test_format_report_contains_source_name(reporter, healthy_metric):
    result = MetricResult(metric=healthy_metric, healthy=True)
    report = PipelineReport(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        source_name="orders-db",
        metric_results=[result],
        active_alerts=[],
    )
    output = reporter.format_report(report)
    assert "orders-db" in output


def test_format_report_shows_healthy_status(reporter, healthy_metric):
    result = MetricResult(metric=healthy_metric, healthy=True)
    report = PipelineReport(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        source_name="orders-db",
        metric_results=[result],
        active_alerts=[],
    )
    output = reporter.format_report(report)
    assert "HEALTHY" in output
    assert "OK" in output


def test_format_report_shows_alert_message(reporter, unhealthy_metric, alert_config):
    result = MetricResult(metric=unhealthy_metric, healthy=False)
    alert = Alert(alert_config=alert_config, message="latency exceeded threshold", triggered_at=datetime(2024, 1, 1, 12, 0, 0))
    report = PipelineReport(
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        source_name="orders-db",
        metric_results=[result],
        active_alerts=[alert],
    )
    output = reporter.format_report(report)
    assert "DEGRADED" in output
    assert "high-latency" in output
    assert "latency exceeded threshold" in output
