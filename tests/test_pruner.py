"""Tests for pipewatch.pruner."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.pruner import PrunePolicy, PruneResult, Pruner
from pipewatch.history import SourceHistory
from pipewatch.checkpoint import CheckpointStore
from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="pipe_a", type="sql", query="SELECT 1", interval=30)


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="row_count", metric="row_count", threshold=10.0, operator="lt")


def _make_result(source_config, alert_config, healthy: bool = True) -> MetricResult:
    from pipewatch.metrics import MetricResult, PipelineMetric
    metric = PipelineMetric(name="row_count", value=20.0 if healthy else 1.0)
    return MetricResult(
        source=source_config,
        metric=metric,
        is_healthy=healthy,
        alerts=[],
    )


@pytest.fixture()
def store(source_config, alert_config) -> SourceHistory:
    h = SourceHistory()
    for _ in range(5):
        h.record(source_config, _make_result(source_config, alert_config))
    return h


# ---------------------------------------------------------------------------
# PrunePolicy validation
# ---------------------------------------------------------------------------

def test_policy_rejects_non_positive_age():
    with pytest.raises(ValueError, match="max_age_seconds"):
        PrunePolicy(max_age_seconds=0)


def test_policy_rejects_non_positive_entries():
    with pytest.raises(ValueError, match="max_entries_per_source"):
        PrunePolicy(max_entries_per_source=0)


def test_policy_accepts_valid_values():
    p = PrunePolicy(max_age_seconds=60.0, max_entries_per_source=10)
    assert p.max_age_seconds == 60.0
    assert p.max_entries_per_source == 10


# ---------------------------------------------------------------------------
# Pruner.prune_history — max_entries_per_source
# ---------------------------------------------------------------------------

def test_prune_history_keeps_n_most_recent(store, source_config):
    pruner = Pruner(PrunePolicy(max_entries_per_source=3))
    result = pruner.prune_history(store)
    assert len(store._store[source_config.name]) == 3
    assert result.snapshots_removed == 2
    assert result.sources_pruned == 1


def test_prune_history_no_change_when_within_limit(store, source_config):
    pruner = Pruner(PrunePolicy(max_entries_per_source=10))
    result = pruner.prune_history(store)
    assert result.snapshots_removed == 0
    assert result.sources_pruned == 0


# ---------------------------------------------------------------------------
# Pruner.prune_history — max_age_seconds
# ---------------------------------------------------------------------------

def test_prune_history_removes_old_snapshots(source_config, alert_config):
    h = SourceHistory()
    old_result = _make_result(source_config, alert_config)
    h.record(source_config, old_result)
    # Manually backdate the snapshot
    h._store[source_config.name][0] = h._store[source_config.name][0].__class__(
        source_name=source_config.name,
        result=old_result,
        collected_at=datetime.now(timezone.utc) - timedelta(seconds=200),
    )
    h.record(source_config, _make_result(source_config, alert_config))  # recent

    pruner = Pruner(PrunePolicy(max_age_seconds=100))
    result = pruner.prune_history(h)
    assert result.snapshots_removed == 1
    assert len(h._store[source_config.name]) == 1


# ---------------------------------------------------------------------------
# Pruner.prune_checkpoints
# ---------------------------------------------------------------------------

def test_prune_checkpoints_removes_stale_entries():
    cp = CheckpointStore()
    cp.update("pipe_a")
    # Backdate the entry
    cp._entries["pipe_a"] = cp._entries["pipe_a"].__class__(
        source_name="pipe_a",
        last_run_at=datetime.now(timezone.utc) - timedelta(seconds=500),
        run_count=1,
    )
    cp.update("pipe_b")  # recent

    pruner = Pruner(PrunePolicy(max_age_seconds=100))
    result = pruner.prune_checkpoints(cp)
    assert result.checkpoints_removed == 1
    assert "pipe_a" not in cp._entries
    assert "pipe_b" in cp._entries


def test_prune_checkpoints_no_age_policy_does_nothing():
    cp = CheckpointStore()
    cp.update("pipe_a")
    pruner = Pruner(PrunePolicy(max_entries_per_source=5))
    result = pruner.prune_checkpoints(cp)
    assert result.checkpoints_removed == 0


# ---------------------------------------------------------------------------
# PruneResult.summary
# ---------------------------------------------------------------------------

def test_prune_result_summary_string():
    r = PruneResult(sources_pruned=2, snapshots_removed=7, checkpoints_removed=1)
    assert "7 snapshot" in r.summary
    assert "2 source" in r.summary
    assert "1 checkpoint" in r.summary
