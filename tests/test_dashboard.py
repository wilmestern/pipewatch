"""Tests for pipewatch.dashboard."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import PipelineMetric, MetricResult
from pipewatch.alerts import Alert
from pipewatch.reporter import PipelineReport
from pipewatch.aggregator import AggregateStats
from pipewatch.trend import TrendResult, TrendDirection
from pipewatch.dashboard import Dashboard, DashboardRow


@pytest.fixture
def source_config():
    return SourceConfig(name="orders_db", type="sql", query="SELECT COUNT(*) FROM orders", interval=60)


@pytest.fixture
def healthy_metric():
    return PipelineMetric(source="orders_db", value=42.0, timestamp=datetime(2024, 1, 1, 12, 0, 0))


@pytest.fixture
def healthy_report(source_config, healthy_metric):
    result = MetricResult(success=True, metric=healthy_metric)
    return PipelineReport(source_name="orders_db", metric_result=result, active_alerts=[])


@pytest.fixture
def unhealthy_report(source_config, healthy_metric):
    result = MetricResult(success=True, metric=healthy_metric)
    alert = Alert(source="orders_db", rule="value < 10", message="Too low", triggered_at=datetime(2024, 1, 1, 12, 0, 0))
    return PipelineReport(source_name="orders_db", metric_result=result, active_alerts=[alert])


@pytest.fixture
def dashboard():
    return Dashboard(title="Test Dashboard")


def test_build_row_returns_dashboard_row(dashboard, healthy_report):
    row = dashboard.build_row(healthy_report)
    assert isinstance(row, DashboardRow)
    assert row.source_name == "orders_db"


def test_build_row_healthy_status(dashboard, healthy_report):
    row = dashboard.build_row(healthy_report)
    assert row.status == "OK"
    assert row.alert_count == 0


def test_build_row_unhealthy_has_alerts(dashboard, unhealthy_report):
    row = dashboard.build_row(unhealthy_report)
    assert row.alert_count == 1


def test_build_row_uses_stats(dashboard, healthy_report):
    stats = AggregateStats(mean=55.5, minimum=10.0, maximum=100.0, count=5, healthy_count=4, unhealthy_count=1)
    row = dashboard.build_row(healthy_report, stats=stats)
    assert row.avg_value == 55.5


def test_build_row_uses_trend(dashboard, healthy_report):
    trend = TrendResult(direction=TrendDirection.RISING, slope=1.2, sample_count=5)
    row = dashboard.build_row(healthy_report, trend=trend)
    assert row.trend == "rising"


def test_build_row_no_stats_no_trend(dashboard, healthy_report):
    row = dashboard.build_row(healthy_report)
    assert row.avg_value is None
    assert row.trend == "insufficient_data"


def test_render_returns_string(dashboard, healthy_report):
    row = dashboard.build_row(healthy_report)
    output = dashboard.render([row])
    assert isinstance(output, str)
    assert "orders_db" in output


def test_render_includes_title(dashboard, healthy_report):
    row = dashboard.build_row(healthy_report)
    output = dashboard.render([row])
    assert "Test Dashboard" in output


def test_render_empty_rows(dashboard):
    output = dashboard.render([])
    assert "No pipeline data available" in output


def test_to_display_line_contains_source(healthy_report, dashboard):
    row = dashboard.build_row(healthy_report)
    line = row.to_display_line()
    assert "orders_db" in line
    assert "OK" in line


def test_render_multiple_rows(dashboard, healthy_report, unhealthy_report):
    row1 = dashboard.build_row(healthy_report)
    row2 = dashboard.build_row(unhealthy_report)
    output = dashboard.render([row1, row2])
    assert output.count("orders_db") == 2
