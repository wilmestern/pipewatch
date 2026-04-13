"""Aggregator module: computes summary statistics over metric history."""

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import SourceHistory


@dataclass
class AggregateStats:
    """Summary statistics for a source's recent metric history."""

    source_name: str
    sample_count: int
    success_rate: float  # 0.0 – 1.0
    avg_row_count: Optional[float]
    min_row_count: Optional[int]
    max_row_count: Optional[int]
    avg_latency_ms: Optional[float]

    @property
    def is_healthy(self) -> bool:
        """Considered healthy when success rate is 100 %."""
        return self.success_rate == 1.0

    def summary(self) -> str:
        return (
            f"[{self.source_name}] samples={self.sample_count} "
            f"success_rate={self.success_rate:.0%} "
            f"avg_rows={self.avg_row_count} "
            f"avg_latency_ms={self.avg_latency_ms}"
        )


class Aggregator:
    """Computes aggregate statistics from a SourceHistory store."""

    def __init__(self, history: SourceHistory) -> None:
        self._history = history

    def compute(self, source_name: str, last_n: Optional[int] = None) -> AggregateStats:
        """Return AggregateStats for *source_name* using up to *last_n* snapshots."""\n        snapshots = self._history.all(source_name)
        if last_n is not None:
            snapshots = snapshots[-last_n:]

        sample_count = len(snapshots)
        if sample_count == 0:
            return AggregateStats(
                source_name=source_name,
                sample_count=0,
                success_rate=0.0,
                avg_row_count=None,
                min_row_count=None,
                max_row_count=None,
                avg_latency_ms=None,
            )

        successes = sum(1 for s in snapshots if s.metric.success)
        success_rate = successes / sample_count

        row_counts = [s.metric.row_count for s in snapshots if s.metric.row_count is not None]
        avg_row_count = sum(row_counts) / len(row_counts) if row_counts else None
        min_row_count = min(row_counts) if row_counts else None
        max_row_count = max(row_counts) if row_counts else None

        latencies = [s.metric.latency_ms for s in snapshots if s.metric.latency_ms is not None]
        avg_latency_ms = sum(latencies) / len(latencies) if latencies else None

        return AggregateStats(
            source_name=source_name,
            sample_count=sample_count,
            success_rate=success_rate,
            avg_row_count=avg_row_count,
            min_row_count=min_row_count,
            max_row_count=max_row_count,
            avg_latency_ms=avg_latency_ms,
        )

    def compute_all(self, last_n: Optional[int] = None) -> list[AggregateStats]:
        """Return AggregateStats for every source tracked in the history store.

        Args:
            last_n: If provided, only the most recent *last_n* snapshots are
                    considered for each source (passed through to :meth:`compute`).

        Returns:
            A list of :class:`AggregateStats`, one per known source, ordered
            alphabetically by source name.
        """
        return [
            self.compute(source_name, last_n=last_n)
            for source_name in sorted(self._history.sources())
        ]
