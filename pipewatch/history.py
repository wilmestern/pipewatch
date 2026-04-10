"""Metric history storage and trend analysis for pipewatch."""

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Deque, Dict, List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class MetricSnapshot:
    """A point-in-time snapshot of a metric result."""
    timestamp: datetime
    result: MetricResult


@dataclass
class SourceHistory:
    """Maintains a rolling history of metric results for a single source."""
    source_name: str
    max_entries: int = 100
    _snapshots: Deque[MetricSnapshot] = field(default_factory=deque, repr=False)

    def record(self, result: MetricResult) -> None:
        """Add a new metric result to the history."""
        snapshot = MetricSnapshot(timestamp=datetime.utcnow(), result=result)
        self._snapshots.append(snapshot)
        if len(self._snapshots) > self.max_entries:
            self._snapshots.popleft()

    def latest(self) -> Optional[MetricSnapshot]:
        """Return the most recent snapshot, or None if empty."""
        return self._snapshots[-1] if self._snapshots else None

    def all(self) -> List[MetricSnapshot]:
        """Return all snapshots in chronological order."""
        return list(self._snapshots)

    def failure_rate(self) -> float:
        """Return the fraction of unhealthy snapshots (0.0 to 1.0)."""
        if not self._snapshots:
            return 0.0
        failures = sum(1 for s in self._snapshots if not s.result.healthy)
        return failures / len(self._snapshots)

    def consecutive_failures(self) -> int:
        """Return the number of consecutive failures from the most recent entry."""
        count = 0
        for snapshot in reversed(list(self._snapshots)):
            if not snapshot.result.healthy:
                count += 1
            else:
                break
        return count


class HistoryStore:
    """Central store for per-source metric history."""

    def __init__(self, max_entries: int = 100) -> None:
        self.max_entries = max_entries
        self._histories: Dict[str, SourceHistory] = {}

    def record(self, source_name: str, result: MetricResult) -> None:
        """Record a metric result for the given source."""
        if source_name not in self._histories:
            self._histories[source_name] = SourceHistory(
                source_name=source_name,
                max_entries=self.max_entries,
            )
        self._histories[source_name].record(result)

    def get(self, source_name: str) -> Optional[SourceHistory]:
        """Retrieve history for a source, or None if not yet recorded."""
        return self._histories.get(source_name)

    def sources(self) -> List[str]:
        """Return a list of all tracked source names."""
        return list(self._histories.keys())
