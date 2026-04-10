"""Tests for pipewatch.exporter."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pipewatch.alerts import Alert
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.exporter import Exporter
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.reporter import PipelineReport


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="db", type="postgres", connection_string="postgresql://localhost/test")


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="high_latency", metric="latency_ms", threshold=500.0, operator="gt")


@pytest.fixture()
def healthy_report(source_config: SourceConfig) -> PipelineReport:
    metric = PipelineMetric(
        source="db",
        latency_ms=120.0,
        error_rate=0.01,
        throughput=200.0,
        collected_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )
    result = MetricResult(source="db", metric=metric, success=True)
    return PipelineReport(source="db", metric_result=result, active_alerts=[])


@pytest.fixture()
def unhealthy_report(source_config: SourceConfig, alert_config: AlertConfig) -> PipelineReport:
    metric = PipelineMetric(
        source="db",
        latency_ms=800.0,
        error_rate=0.25,
        throughput=50.0,
        collected_at=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
    )
    result = MetricResult(source="db", metric=metric, success=True)
    alert = Alert(config=alert_config, source="db", current_value=800.0)
    return PipelineReport(source="db", metric_result=result, active_alerts=[alert])


@pytest.fixture()
def exporter() -> Exporter:
    return Exporter()


# ---------------------------------------------------------------------------
# JSON export tests
# ---------------------------------------------------------------------------


def test_to_json_returns_string(exporter: Exporter, healthy_report: PipelineReport) -> None:
    result = exporter.to_json([healthy_report])
    assert isinstance(result, str)


def test_to_json_is_valid_json(exporter: Exporter, healthy_report: PipelineReport) -> None:
    result = exporter.to_json([healthy_report])
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert len(parsed) == 1


def test_to_json_contains_expected_fields(exporter: Exporter, healthy_report: PipelineReport) -> None:
    parsed = json.loads(exporter.to_json([healthy_report]))
    record = parsed[0]
    assert record["source"] == "db"
    assert record["is_healthy"] is True
    assert record["latency_ms"] == 120.0
    assert record["active_alerts"] == 0


def test_to_json_multiple_reports(exporter: Exporter, healthy_report, unhealthy_report) -> None:
    parsed = json.loads(exporter.to_json([healthy_report, unhealthy_report]))
    assert len(parsed) == 2


def test_to_json_empty_list(exporter: Exporter) -> None:
    result = exporter.to_json([])
    assert json.loads(result) == []


# ---------------------------------------------------------------------------
# CSV export tests
# ---------------------------------------------------------------------------


def test_to_csv_returns_string(exporter: Exporter, healthy_report: PipelineReport) -> None:
    result = exporter.to_csv([healthy_report])
    assert isinstance(result, str)


def test_to_csv_has_header(exporter: Exporter, healthy_report: PipelineReport) -> None:
    result = exporter.to_csv([healthy_report])
    first_line = result.splitlines()[0]
    assert "source" in first_line
    assert "latency_ms" in first_line


def test_to_csv_data_row_count(exporter: Exporter, healthy_report, unhealthy_report) -> None:
    result = exporter.to_csv([healthy_report, unhealthy_report])
    lines = [l for l in result.splitlines() if l.strip()]
    assert len(lines) == 3  # header + 2 data rows


def test_to_csv_empty_list_returns_empty_string(exporter: Exporter) -> None:
    assert exporter.to_csv([]) == ""


def test_to_csv_unhealthy_alert_count(exporter: Exporter, unhealthy_report: PipelineReport) -> None:
    result = exporter.to_csv([unhealthy_report])
    data_line = result.splitlines()[1]
    assert "1" in data_line  # one active alert
