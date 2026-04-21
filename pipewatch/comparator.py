"""Comparator: compare metric results across two time windows or sources."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pipewatch.history import SourceHistory
from pipewatch.metrics import MetricResult


@dataclass(frozen=True)
class ComparisonResult:
    source_name: str
    baseline_value: float
    current_value: float
    delta: float
    delta_pct: Optional[float]  # None when baseline is zero
    improved: bool

    def summary(self) -> str:
        pct = f"{self.delta_pct:+.1f}%" if self.delta_pct is not None else "N/A"
        direction = "improved" if self.improved else "degraded"
        return (
            f"{self.source_name}: {self.baseline_value:.3f} -> {self.current_value:.3f} "
            f"({pct}, {direction})"
        )


class Comparator:
    """Compare the most-recent metric value against a historical window average."""

    def __init__(self, history: SourceHistory, window_size: int = 10) -> None:
        if window_size < 1:
            raise ValueError("window_size must be at least 1")
        self._history = history
        self._window_size = window_size

    def compare(self, source_name: str) -> Optional[ComparisonResult]:
        """Return a ComparisonResult for *source_name*, or None if insufficient data."""
        snapshots = self._history.recent(source_name, self._window_size + 1)
        if len(snapshots) < 2:
            return None

        current_value = snapshots[-1].result.value
        baseline_values = [s.result.value for s in snapshots[:-1]]
        baseline_value = sum(baseline_values) / len(baseline_values)

        delta = current_value - baseline_value
        delta_pct = (delta / baseline_value * 100.0) if baseline_value != 0.0 else None
        # Lower latency / error counts are better; treat a decrease as improvement
        improved = delta <= 0

        return ComparisonResult(
            source_name=source_name,
            baseline_value=baseline_value,
            current_value=current_value,
            delta=delta,
            delta_pct=delta_pct,
            improved=improved,
        )

    def compare_all(self) -> list[ComparisonResult]:
        """Return ComparisonResults for every source that has enough history."""
        results = []
        for name in self._history.source_names():
            r = self.compare(name)
            if r is not None:
                results.append(r)
        return results
