"""Example: using Pruner to keep history and checkpoints lean."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.history import SourceHistory
from pipewatch.checkpoint import CheckpointStore
from pipewatch.pruner import PrunePolicy, Pruner


def _make_result(source: SourceConfig, healthy: bool = True) -> MetricResult:
    metric = PipelineMetric(name="row_count", value=50.0 if healthy else 0.0)
    return MetricResult(source=source, metric=metric, is_healthy=healthy, alerts=[])


def run_demo() -> None:
    source = SourceConfig(name="orders_db", type="sql", query="SELECT COUNT(*) FROM orders", interval=60)

    # --- Build a history store with 10 entries ---
    history = SourceHistory()
    for i in range(10):
        history.record(source, _make_result(source, healthy=(i % 3 != 0)))

    print(f"Before pruning: {len(history._store[source.name])} snapshots")

    # --- Build a checkpoint store with two sources, one stale ---
    checkpoints = CheckpointStore()
    checkpoints.update("orders_db")
    checkpoints.update("legacy_pipe")
    # Simulate legacy_pipe not running for a long time
    from pipewatch.checkpoint import CheckpointEntry
    checkpoints._entries["legacy_pipe"] = CheckpointEntry(
        source_name="legacy_pipe",
        last_run_at=datetime.now(timezone.utc) - timedelta(hours=2),
        run_count=42,
    )

    print(f"Before pruning: {len(checkpoints._entries)} checkpoint(s)")

    # --- Apply pruning policy ---
    policy = PrunePolicy(
        max_age_seconds=3600,       # drop anything older than 1 hour
        max_entries_per_source=5,   # keep at most 5 snapshots per source
    )
    pruner = Pruner(policy)

    history_result = pruner.prune_history(history)
    checkpoint_result = pruner.prune_checkpoints(checkpoints)

    print(f"After pruning:  {len(history._store[source.name])} snapshots")
    print(f"After pruning:  {len(checkpoints._entries)} checkpoint(s)")
    print(history_result.summary)
    print(checkpoint_result.summary)


if __name__ == "__main__":
    run_demo()
