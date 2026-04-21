"""windower.py — Sliding window aggregation over metric history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.history import SourceHistory
from pipewatch.metrics import MetricResult


@dataclass
class WindowStats:
    source_name: str
    window_seconds: int
    sample_count: int
    healthy_count: int
    unhealthy_count: int
    avg_value: Optional[float]
    min_value: Optional[float]
    max_value: Optional[float]

    @property
    def health_rate(self) -> float:
        """Fraction of healthy samples in the window (0.0–1.0)."""
        if self.sample_count == 0:
            return 1.0
        return self.healthy_count / self.sample_count

    def summary(self) -> str:
        pct = f"{self.health_rate * 100:.1f}%"
        avg = f"{self.avg_value:.4f}" if self.avg_value is not None else "n/a"
        return (
            f"[{self.source_name}] window={self.window_seconds}s "
            f"samples={self.sample_count} healthy={pct} avg={avg}"
        )


class Windower:
    """Computes sliding-window statistics from a SourceHistory store."""

    def __init__(self, store: SourceHistory) -> None:
        self._store = store

    def compute(self, source_name: str, window_seconds: int) -> WindowStats:
        """Return aggregated stats for *source_name* over the last *window_seconds*."""
        cutoff = datetime.utcnow() - timedelta(seconds=window_seconds)
        snapshots = self._store.snapshots(source_name)
        in_window: List[MetricResult] = [
            s.result for s in snapshots if s.recorded_at >= cutoff
        ]

        healthy = sum(1 for r in in_window if r.is_healthy)
        values = [r.metric.value for r in in_window if r.metric.value is not None]

        return WindowStats(
            source_name=source_name,
            window_seconds=window_seconds,
            sample_count=len(in_window),
            healthy_count=healthy,
            unhealthy_count=len(in_window) - healthy,
            avg_value=sum(values) / len(values) if values else None,
            min_value=min(values) if values else None,
            max_value=max(values) if values else None,
        )
