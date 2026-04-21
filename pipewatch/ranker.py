"""ranker.py — Ranks pipeline sources by health score, alert frequency, and recency.

Provides a sorted view of sources so operators can quickly identify which
pipelines need the most attention.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.scorer import ScoreReport, SourceScore
from pipewatch.history import SourceHistory
from pipewatch.alerts import Alert


@dataclass
class RankedSource:
    """A single source entry in a ranked list."""

    source_name: str
    score: float  # 0.0 – 1.0, higher is healthier
    grade: str
    alert_count: int
    latest_value: Optional[float]
    rank: int = 0  # populated by Ranker

    @property
    def summary(self) -> str:
        return (
            f"#{self.rank} {self.source_name} "
            f"score={self.score:.2f} ({self.grade}) "
            f"alerts={self.alert_count}"
        )


@dataclass
class RankReport:
    """Ordered list of ranked sources, worst-first by default."""

    rows: List[RankedSource] = field(default_factory=list)

    @property
    def worst(self) -> Optional[RankedSource]:
        """Return the lowest-ranked (most concerning) source."""
        return self.rows[0] if self.rows else None

    @property
    def best(self) -> Optional[RankedSource]:
        """Return the highest-ranked (healthiest) source."""
        return self.rows[-1] if self.rows else None

    def top_n(self, n: int) -> List[RankedSource]:
        """Return the n worst-ranked sources."""
        return self.rows[:n]

    @property
    def summary(self) -> str:
        lines = [f"Rank Report ({len(self.rows)} sources):"]
        for row in self.rows:
            lines.append(f"  {row.summary}")
        return "\n".join(lines)


class Ranker:
    """Ranks sources using score data from ScoreReport and alert counts.

    Sources are sorted ascending by score (worst first), with alert count
    used as a tiebreaker (more alerts → worse rank).
    """

    def rank(
        self,
        score_report: ScoreReport,
        active_alerts: Optional[List[Alert]] = None,
        history: Optional[SourceHistory] = None,
    ) -> RankReport:
        """Produce a RankReport from a ScoreReport.

        Args:
            score_report: Computed score data for each source.
            active_alerts: Optional list of currently active alerts used
                           to supplement alert counts per source.
            history: Optional SourceHistory used to look up the latest
                     metric value for each source.

        Returns:
            A RankReport with sources ordered worst-first.
        """
        # Build a per-source alert count from active_alerts if provided.
        alert_counts: dict[str, int] = {}
        if active_alerts:
            for alert in active_alerts:
                alert_counts[alert.source_name] = (
                    alert_counts.get(alert.source_name, 0) + 1
                )

        ranked: List[RankedSource] = []
        for source_score in score_report.scores:
            latest_value: Optional[float] = None
            if history is not None:
                snapshot = history.latest(source_score.source_name)
                if snapshot is not None:
                    latest_value = snapshot.value

            ranked.append(
                RankedSource(
                    source_name=source_score.source_name,
                    score=source_score.score,
                    grade=source_score.grade,
                    alert_count=alert_counts.get(source_score.source_name, 0),
                    latest_value=latest_value,
                )
            )

        # Sort: lowest score first; break ties by most alerts first.
        ranked.sort(key=lambda r: (r.score, -r.alert_count))

        # Assign 1-based rank positions.
        for idx, row in enumerate(ranked, start=1):
            row.rank = idx

        return RankReport(rows=ranked)
