"""Snapshot module: capture and compare point-in-time pipeline state."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class PipelineSnapshot:
    """A point-in-time capture of metric results across all sources."""

    captured_at: datetime
    results: Dict[str, MetricResult] = field(default_factory=dict)

    @property
    def source_names(self) -> List[str]:
        return list(self.results.keys())

    @property
    def healthy_count(self) -> int:
        return sum(1 for r in self.results.values() if r.is_healthy)

    @property
    def unhealthy_count(self) -> int:
        return len(self.results) - self.healthy_count

    def summary(self) -> str:
        total = len(self.results)
        ts = self.captured_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        return (
            f"[{ts}] {total} source(s): "
            f"{self.healthy_count} healthy, {self.unhealthy_count} unhealthy"
        )


@dataclass
class SnapshotDiff:
    """Difference between two consecutive snapshots."""

    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    flipped_unhealthy: List[str] = field(default_factory=list)
    flipped_healthy: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(
            self.added or self.removed
            or self.flipped_unhealthy or self.flipped_healthy
        )

    def summary(self) -> str:
        parts = []
        if self.added:
            parts.append(f"added={self.added}")
        if self.removed:
            parts.append(f"removed={self.removed}")
        if self.flipped_unhealthy:
            parts.append(f"degraded={self.flipped_unhealthy}")
        if self.flipped_healthy:
            parts.append(f"recovered={self.flipped_healthy}")
        return "; ".join(parts) if parts else "no changes"


class SnapshotManager:
    """Captures pipeline snapshots and computes diffs between them."""

    def __init__(self) -> None:
        self._snapshots: List[PipelineSnapshot] = []

    def capture(self, results: Dict[str, MetricResult]) -> PipelineSnapshot:
        """Record a new snapshot from the given results mapping."""
        snap = PipelineSnapshot(
            captured_at=datetime.now(tz=timezone.utc),
            results=dict(results),
        )
        self._snapshots.append(snap)
        return snap

    def latest(self) -> Optional[PipelineSnapshot]:
        """Return the most recent snapshot, or None."""
        return self._snapshots[-1] if self._snapshots else None

    def previous(self) -> Optional[PipelineSnapshot]:
        """Return the second-most-recent snapshot, or None."""
        return self._snapshots[-2] if len(self._snapshots) >= 2 else None

    def diff(self) -> Optional[SnapshotDiff]:
        """Compute the diff between the two most recent snapshots."""
        prev = self.previous()
        curr = self.latest()
        if prev is None or curr is None:
            return None
        prev_keys = set(prev.results)
        curr_keys = set(curr.results)
        d = SnapshotDiff(
            added=sorted(curr_keys - prev_keys),
            removed=sorted(prev_keys - curr_keys),
        )
        for key in prev_keys & curr_keys:
            was_healthy = prev.results[key].is_healthy
            is_healthy = curr.results[key].is_healthy
            if was_healthy and not is_healthy:
                d.flipped_unhealthy.append(key)
            elif not was_healthy and is_healthy:
                d.flipped_healthy.append(key)
        return d

    def history(self) -> List[PipelineSnapshot]:
        """Return all captured snapshots in chronological order."""
        return list(self._snapshots)
