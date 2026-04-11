"""Tests for pipewatch.digest."""

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.history import SourceHistory
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.digest import DigestBuilder, DigestReport, SourceDigest


@pytest.fixture()
def source_config():
    return SourceConfig(name="pipe-a", url="http://example.com", interval=30)


@pytest.fixture()
def alert_config():
    return AlertConfig(metric="latency_ms", threshold=500.0, operator="gt")


def _make_result(source_config, alert_config, healthy: bool, latency: float = 100.0):
    metric = PipelineMetric(
        source_name=source_config.name,
        latency_ms=latency,
        record_count=10,
        error_count=0 if healthy else 5,
        timestamp=datetime.utcnow(),
    )
    return MetricResult(
        source=source_config,
        metric=metric,
        alerts=[alert_config] if not healthy else [],
        collected_at=datetime.utcnow(),
    )


@pytest.fixture()
def populated_history(source_config, alert_config):
    h = SourceHistory(source_name=source_config.name)
    for i in range(5):
        h.record(_make_result(source_config, alert_config, healthy=True, latency=100.0 + i * 10))
    return h


def test_digest_report_counts(source_config, alert_config, populated_history):
    builder = DigestBuilder(histories={source_config.name: populated_history})
    report = builder.build()
    assert report.total_sources == 1
    assert report.healthy_count == 1
    assert report.unhealthy_count == 0


def test_digest_report_summary_contains_source_name(source_config, populated_history):
    builder = DigestBuilder(histories={source_config.name: populated_history})
    report = builder.build()
    assert source_config.name in report.summary()


def test_digest_report_summary_contains_header(source_config, populated_history):
    builder = DigestBuilder(histories={source_config.name: populated_history})
    report = builder.build()
    assert "Pipewatch Digest" in report.summary()


def test_empty_history_excluded(source_config):
    empty_history = SourceHistory(source_name=source_config.name)
    builder = DigestBuilder(histories={source_config.name: empty_history})
    report = builder.build()
    assert report.total_sources == 0


def test_source_digest_summary_line(source_config, alert_config, populated_history):
    builder = DigestBuilder(histories={source_config.name: populated_history})
    report = builder.build()
    assert len(report.sources) == 1
    line = report.sources[0].summary_line
    assert "success_rate" in line
    assert "avg_latency" in line


def test_multiple_sources(source_config, alert_config):
    histories = {}
    for i in range(3):
        name = f"pipe-{i}"
        cfg = SourceConfig(name=name, url="http://x.com", interval=30)
        acfg = AlertConfig(metric="latency_ms", threshold=500.0, operator="gt")
        h = SourceHistory(source_name=name)
        for _ in range(4):
            h.record(_make_result(cfg, acfg, healthy=(i != 2)))
        histories[name] = h
    builder = DigestBuilder(histories=histories)
    report = builder.build()
    assert report.total_sources == 3
    assert report.unhealthy_count == 1


def test_trend_included_when_enough_samples(source_config, populated_history):
    builder = DigestBuilder(histories={source_config.name: populated_history}, min_samples=3)
    report = builder.build()
    assert report.sources[0].trend is not None


def test_digest_generated_at_is_recent():
    builder = DigestBuilder(histories={})
    before = datetime.utcnow()
    report = builder.build()
    after = datetime.utcnow()
    assert before <= report.generated_at <= after
