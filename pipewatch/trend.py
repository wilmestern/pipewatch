"""Trend analysis for pipeline metrics over time."""
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from pipewatch.history import SourceHistory, MetricSnapshot


class TrendDirection(str, Enum):
    IMPROVING = "improving"
    DEGRADING = "degrading"
    STABLE = "stable"
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass
class TrendResult:
    source_name: str
    direction: TrendDirection
    average_latency: Optional[float]
    error_rate: float  # 0.0 - 1.0
    sample_count: int

    @property
    def summary(self) -> str:
        if self.direction == TrendDirection.INSUFFICIENT_DATA:
            return f"{self.source_name}: insufficient data for trend analysis"
        return (
            f"{self.source_name}: {self.direction.value} | "
            f"avg_latency={self.average_latency:.2f}s | "
            f"error_rate={self.error_rate:.1%} | "
            f"samples={self.sample_count}"
        )


class TrendAnalyzer:
    """Analyzes metric history to detect performance trends."""

    MIN_SAMPLES = 3

    def __init__(self, history: SourceHistory, window: int = 10) -> None:
        self._history = history
        self._window = window

    def analyze(self, source_name: str) -> TrendResult:
        snapshots: List[MetricSnapshot] = self._history.recent(source_name, self._window)

        if len(snapshots) < self.MIN_SAMPLES:
            return TrendResult(
                source_name=source_name,
                direction=TrendDirection.INSUFFICIENT_DATA,
                average_latency=None,
                error_rate=0.0,
                sample_count=len(snapshots),
            )

        latencies = [
            s.metric.latency_seconds
            for s in snapshots
            if s.metric.latency_seconds is not None
        ]
        avg_latency = sum(latencies) / len(latencies) if latencies else None
        error_rate = sum(1 for s in snapshots if not s.healthy) / len(snapshots)

        direction = self._compute_direction(snapshots)

        return TrendResult(
            source_name=source_name,
            direction=direction,
            average_latency=avg_latency,
            error_rate=error_rate,
            sample_count=len(snapshots),
        )

    def _compute_direction(self, snapshots: List[MetricSnapshot]) -> TrendDirection:
        half = len(snapshots) // 2
        older = snapshots[:half]
        newer = snapshots[half:]

        older_errors = sum(1 for s in older if not s.healthy)
        newer_errors = sum(1 for s in newer if not s.healthy)

        if newer_errors < older_errors:
            return TrendDirection.IMPROVING
        if newer_errors > older_errors:
            return TrendDirection.DEGRADING
        return TrendDirection.STABLE
