"""Pruner: removes stale or excess entries from history and checkpoint stores."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from pipewatch.history import SourceHistory
from pipewatch.checkpoint import CheckpointStore


@dataclass
class PrunePolicy:
    """Defines how aggressively to prune stored data."""
    max_age_seconds: Optional[float] = None  # remove entries older than this
    max_entries_per_source: Optional[int] = None  # keep only the N most recent

    def __post_init__(self) -> None:
        if self.max_age_seconds is not None and self.max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        if self.max_entries_per_source is not None and self.max_entries_per_source <= 0:
            raise ValueError("max_entries_per_source must be a positive integer")


@dataclass
class PruneResult:
    """Summary of a single pruning run."""
    sources_pruned: int = 0
    snapshots_removed: int = 0
    checkpoints_removed: int = 0
    ran_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def summary(self) -> str:
        return (
            f"Pruned {self.snapshots_removed} snapshot(s) across "
            f"{self.sources_pruned} source(s); "
            f"{self.checkpoints_removed} checkpoint(s) removed."
        )


class Pruner:
    """Applies a PrunePolicy to SourceHistory and CheckpointStore instances."""

    def __init__(self, policy: PrunePolicy) -> None:
        self._policy = policy

    def prune_history(self, history: SourceHistory) -> PruneResult:
        """Remove old or excess snapshots from a SourceHistory store."""
        result = PruneResult()
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=self._policy.max_age_seconds)
            if self._policy.max_age_seconds is not None
            else None
        )

        for source_name in list(history._store.keys()):
            snapshots = history._store[source_name]
            original_count = len(snapshots)

            if cutoff is not None:
                snapshots = [s for s in snapshots if s.collected_at >= cutoff]

            if self._policy.max_entries_per_source is not None:
                snapshots = snapshots[-self._policy.max_entries_per_source:]

            removed = original_count - len(snapshots)
            if removed > 0:
                history._store[source_name] = snapshots
                result.sources_pruned += 1
                result.snapshots_removed += removed

        return result

    def prune_checkpoints(self, store: CheckpointStore) -> PruneResult:
        """Remove stale checkpoint entries based on age policy."""
        result = PruneResult()
        if self._policy.max_age_seconds is None:
            return result

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._policy.max_age_seconds)
        stale_keys = [
            k for k, v in store._entries.items()
            if v.last_run_at < cutoff
        ]
        for k in stale_keys:
            del store._entries[k]
            result.checkpoints_removed += 1

        return result
