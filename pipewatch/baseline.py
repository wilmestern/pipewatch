"""Baseline tracking for pipeline metrics.

Compares current metric values against a learned or configured baseline
to detect anomalies relative to expected behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from pipewatch.metrics import MetricResult


@dataclass
class BaselineEntry:
    """Stores the reference baseline value for a single source."""

    source_name: str
    expected_value: float
    tolerance: float = 0.10  # fractional tolerance, e.g. 0.10 = 10%

    @property
    def lower_bound(self) -> float:
        return self.expected_value * (1.0 - self.tolerance)

    @property
    def upper_bound(self) -> float:
        return self.expected_value * (1.0 + self.tolerance)

    def within_baseline(self, value: float) -> bool:
        """Return True if *value* falls within the acceptable range."""
        return self.lower_bound <= value <= self.upper_bound


@dataclass
class BaselineResult:
    """Outcome of a baseline comparison for one metric result."""

    source_name: str
    observed: float
    expected: float
    within_baseline: bool
    deviation_pct: float

    @property
    def summary(self) -> str:
        direction = "above" if self.observed > self.expected else "below"
        status = "OK" if self.within_baseline else "ANOMALY"
        return (
            f"[{status}] {self.source_name}: observed={self.observed:.4f} "
            f"expected={self.expected:.4f} "
            f"({abs(self.deviation_pct):.1f}% {direction} baseline)"
        )


class BaselineTracker:
    """Compares MetricResults against registered baseline entries."""

    def __init__(self) -> None:
        self._baselines: Dict[str, BaselineEntry] = {}

    def register(self, entry: BaselineEntry) -> None:
        """Register or overwrite a baseline entry for a source."""
        self._baselines[entry.source_name] = entry

    def get(self, source_name: str) -> Optional[BaselineEntry]:
        return self._baselines.get(source_name)

    def compare(self, result: MetricResult) -> Optional[BaselineResult]:
        """Compare *result* against its baseline.  Returns None if no baseline registered."""
        entry = self._baselines.get(result.source_name)
        if entry is None:
            return None

        observed = result.metric.value
        expected = entry.expected_value
        deviation_pct = ((observed - expected) / expected * 100.0) if expected != 0 else 0.0

        return BaselineResult(
            source_name=result.source_name,
            observed=observed,
            expected=expected,
            within_baseline=entry.within_baseline(observed),
            deviation_pct=deviation_pct,
        )

    def compare_all(self, results: list[MetricResult]) -> list[BaselineResult]:
        """Return baseline comparisons for all results that have a registered baseline."""
        out: list[BaselineResult] = []
        for r in results:
            br = self.compare(r)
            if br is not None:
                out.append(br)
        return out
