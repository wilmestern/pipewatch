"""Pipeline health scorer — computes a numeric health score for each source
based on recent metric history and alert activity."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import SourceHistory
from pipewatch.alerts import Alert


@dataclass
class SourceScore:
    source_name: str
    score: float  # 0.0 (worst) to 1.0 (perfect)
    total_samples: int
    healthy_samples: int
    active_alert_count: int

    @property
    def grade(self) -> str:
        """Letter grade derived from the numeric score."""
        if self.score >= 0.90:
            return "A"
        if self.score >= 0.75:
            return "B"
        if self.score >= 0.50:
            return "C"
        if self.score >= 0.25:
            return "D"
        return "F"

    def summary(self) -> str:
        return (
            f"{self.source_name}: score={self.score:.2f} grade={self.grade} "
            f"healthy={self.healthy_samples}/{self.total_samples} "
            f"active_alerts={self.active_alert_count}"
        )


@dataclass
class ScoreReport:
    scores: List[SourceScore] = field(default_factory=list)

    @property
    def average_score(self) -> float:
        if not self.scores:
            return 0.0
        return sum(s.score for s in self.scores) / len(self.scores)

    def for_source(self, source_name: str) -> Optional[SourceScore]:
        for s in self.scores:
            if s.source_name == source_name:
                return s
        return None


class Scorer:
    """Computes health scores from a SourceHistory and active alerts."""

    def __init__(self, history: SourceHistory, alert_weight: float = 0.2) -> None:
        if not (0.0 <= alert_weight <= 1.0):
            raise ValueError("alert_weight must be between 0.0 and 1.0")
        self._history = history
        self._alert_weight = alert_weight

    def score_source(self, source_name: str, active_alerts: List[Alert]) -> SourceScore:
        snapshots = self._history.all(source_name)
        total = len(snapshots)
        healthy = sum(1 for s in snapshots if s.is_healthy)

        availability = healthy / total if total > 0 else 0.0

        alert_penalty = min(len(active_alerts) * 0.1, 1.0)
        score = max(0.0, availability * (1.0 - self._alert_weight) +
                    (1.0 - alert_penalty) * self._alert_weight)

        return SourceScore(
            source_name=source_name,
            score=round(score, 4),
            total_samples=total,
            healthy_samples=healthy,
            active_alert_count=len(active_alerts),
        )

    def compute(self, alerts_by_source: dict[str, List[Alert]]) -> ScoreReport:
        source_names = self._history.source_names()
        scores = [
            self.score_source(name, alerts_by_source.get(name, []))
            for name in source_names
        ]
        return ScoreReport(scores=scores)
