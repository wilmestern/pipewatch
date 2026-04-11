"""Digest module: periodic summary reports aggregating pipeline health across sources."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pipewatch.aggregator import AggregateStats, Aggregator
from pipewatch.history import SourceHistory
from pipewatch.trend import TrendAnalyzer, TrendResult


@dataclass
class SourceDigest:
    source_name: str
    stats: AggregateStats
    trend: Optional[TrendResult] = None

    @property
    def summary_line(self) -> str:
        trend_label = f" | trend={self.trend.direction.value}" if self.trend else ""
        return (
            f"[{self.source_name}] "
            f"healthy={self.stats.is_healthy} "
            f"success_rate={self.stats.success_rate:.1%} "
            f"avg_latency={self.stats.avg_latency_ms:.1f}ms"
            f"{trend_label}"
        )


@dataclass
class DigestReport:
    generated_at: datetime
    sources: List[SourceDigest] = field(default_factory=list)

    @property
    def total_sources(self) -> int:
        return len(self.sources)

    @property
    def healthy_count(self) -> int:
        return sum(1 for s in self.sources if s.stats.is_healthy)

    @property
    def unhealthy_count(self) -> int:
        return self.total_sources - self.healthy_count

    def summary(self) -> str:
        lines = [
            f"=== Pipewatch Digest [{self.generated_at.isoformat()}] ===",
            f"Sources: {self.total_sources} total, "
            f"{self.healthy_count} healthy, {self.unhealthy_count} unhealthy",
        ]
        for src in self.sources:
            lines.append(f"  {src.summary_line}")
        return "\n".join(lines)


class DigestBuilder:
    """Builds a DigestReport from history stores for all tracked sources."""

    def __init__(self, histories: dict[str, SourceHistory], min_samples: int = 3) -> None:
        self._histories = histories
        self._min_samples = min_samples

    def build(self) -> DigestReport:
        sources: List[SourceDigest] = []
        for name, history in self._histories.items():
            aggregator = Aggregator(history)
            stats = aggregator.compute()
            if stats is None:
                continue
            analyzer = TrendAnalyzer(history, min_samples=self._min_samples)
            trend = analyzer.analyze()
            sources.append(SourceDigest(source_name=name, stats=stats, trend=trend))
        return DigestReport(generated_at=datetime.utcnow(), sources=sources)
