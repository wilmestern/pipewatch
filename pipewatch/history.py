"""Metric history storage and retrieval for pipeline sources."""
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Deque, List, Optional

from pipewatch.metrics import MetricResult, PipelineMetric


@dataclass
class MetricSnapshot:
    source_name: str
    metric: PipelineMetric
    healthy: bool
    recorded_at: datetime = field(default_factory=datetime.utcnow)


class SourceHistory:
    """In-memory rolling history of metric snapshots per source."""

    def __init__(self, max_entries: int = 100) -> None:
        self._max_entries = max_entries
        self._store: Dict[str, Deque[MetricSnapshot]] = defaultdict(
            lambda: deque(maxlen=self._max_entries)
        )

    def record(self, result: MetricResult) -> None:
        snapshot = MetricSnapshot(
            source_name=result.source_name,
            metric=result.metric,
            healthy=result.healthy,
        )
        self._store[result.source_name].append(snapshot)

    def latest(self, source_name: str) -> Optional[MetricSnapshot]:
        entries = self._store.get(source_name)
        if not entries:
            return None
        return entries[-1]

    def all(self, source_name: str) -> List[MetricSnapshot]:
        return list(self._store.get(source_name, []))

    def recent(self, source_name: str, n: int) -> List[MetricSnapshot]:
        entries = self._store.get(source_name)
        if not entries:
            return []
        return list(entries)[-n:]

    def sources(self) -> List[str]:
        return list(self._store.keys())

    def clear(self, source_name: str) -> None:
        if source_name in self._store:
            self._store[source_name].clear()

    def error_rate(self, source_name: str, window: int = 10) -> float:
        snapshots = self.recent(source_name, window)
        if not snapshots:
            return 0.0
        return sum(1 for s in snapshots if not s.healthy) / len(snapshots)

    def uptime(self, source_name: str) -> float:
        all_snapshots = self.all(source_name)
        if not all_snapshots:
            return 1.0
        healthy_count = sum(1 for s in all_snapshots if s.healthy)
        return healthy_count / len(all_snapshots)

    def average_latency(self, source_name: str, window: int = 10) -> Optional[float]:
        snapshots = self.recent(source_name, window)
        latencies = [
            s.metric.latency_seconds
            for s in snapshots
            if s.metric.latency_seconds is not None
        ]
        if not latencies:
            return None
        return sum(latencies) / len(latencies)
