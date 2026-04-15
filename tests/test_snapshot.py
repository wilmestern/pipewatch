"""Tests for pipewatch.snapshot."""
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult
from pipewatch.snapshot import PipelineSnapshot, SnapshotDiff, SnapshotManager


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="db", type="postgres", connection="postgresql://localhost/test")


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="row_count", metric="row_count", threshold=100, operator="gte")


def _make_result(source: SourceConfig, alert: AlertConfig, healthy: bool) -> MetricResult:
    return MetricResult(
        source=source,
        alert=alert,
        value=200.0 if healthy else 10.0,
        is_healthy=healthy,
        collected_at=datetime.now(tz=timezone.utc),
    )


@pytest.fixture()
def manager() -> SnapshotManager:
    return SnapshotManager()


def test_latest_returns_none_when_empty(manager):
    assert manager.latest() is None


def test_previous_returns_none_when_empty(manager):
    assert manager.previous() is None


def test_capture_stores_snapshot(manager, source_config, alert_config):
    results = {"db": _make_result(source_config, alert_config, True)}
    snap = manager.capture(results)
    assert isinstance(snap, PipelineSnapshot)
    assert manager.latest() is snap


def test_snapshot_healthy_count(source_config, alert_config):
    results = {
        "db": _make_result(source_config, alert_config, True),
        "api": _make_result(source_config, alert_config, False),
    }
    snap = PipelineSnapshot(captured_at=datetime.now(tz=timezone.utc), results=results)
    assert snap.healthy_count == 1
    assert snap.unhealthy_count == 1


def test_snapshot_summary_contains_counts(source_config, alert_config):
    results = {"db": _make_result(source_config, alert_config, True)}
    snap = PipelineSnapshot(captured_at=datetime.now(tz=timezone.utc), results=results)
    summary = snap.summary()
    assert "1 source" in summary
    assert "1 healthy" in summary
    assert "0 unhealthy" in summary


def test_diff_returns_none_with_single_snapshot(manager, source_config, alert_config):
    manager.capture({"db": _make_result(source_config, alert_config, True)})
    assert manager.diff() is None


def test_diff_detects_added_source(manager, source_config, alert_config):
    manager.capture({"db": _make_result(source_config, alert_config, True)})
    manager.capture({
        "db": _make_result(source_config, alert_config, True),
        "api": _make_result(source_config, alert_config, True),
    })
    diff = manager.diff()
    assert diff is not None
    assert "api" in diff.added
    assert diff.has_changes


def test_diff_detects_removed_source(manager, source_config, alert_config):
    manager.capture({
        "db": _make_result(source_config, alert_config, True),
        "api": _make_result(source_config, alert_config, True),
    })
    manager.capture({"db": _make_result(source_config, alert_config, True)})
    diff = manager.diff()
    assert diff is not None
    assert "api" in diff.removed


def test_diff_detects_flipped_unhealthy(manager, source_config, alert_config):
    manager.capture({"db": _make_result(source_config, alert_config, True)})
    manager.capture({"db": _make_result(source_config, alert_config, False)})
    diff = manager.diff()
    assert diff is not None
    assert "db" in diff.flipped_unhealthy
    assert "db" not in diff.flipped_healthy


def test_diff_detects_flipped_healthy(manager, source_config, alert_config):
    manager.capture({"db": _make_result(source_config, alert_config, False)})
    manager.capture({"db": _make_result(source_config, alert_config, True)})
    diff = manager.diff()
    assert diff is not None
    assert "db" in diff.flipped_healthy


def test_diff_no_changes_summary(source_config, alert_config):
    d = SnapshotDiff()
    assert d.summary() == "no changes"
    assert not d.has_changes


def test_history_returns_all_snapshots(manager, source_config, alert_config):
    manager.capture({"db": _make_result(source_config, alert_config, True)})
    manager.capture({"db": _make_result(source_config, alert_config, False)})
    assert len(manager.history()) == 2
