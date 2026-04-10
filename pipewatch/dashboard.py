"""Terminal dashboard for displaying pipeline health summaries."""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

from pipewatch.reporter import PipelineReport
from pipewatch.aggregator import AggregateStats
from pipewatch.trend import TrendResult, TrendDirection


@dataclass
class DashboardRow:
    source_name: str
    status: str
    last_value: Optional[float]
    avg_value: Optional[float]
    trend: str
    alert_count: int
    last_checked: Optional[str]

    def to_display_line(self) -> str:
        trend_symbol = {
            "rising": "↑",
            "falling": "↓",
            "stable": "→",
            "insufficient_data": "?",
        }.get(self.trend, "?")
        last_val = f"{self.last_value:.2f}" if self.last_value is not None else "N/A"
        avg_val = f"{self.avg_value:.2f}" if self.avg_value is not None else "N/A"
        return (
            f"[{self.status:<8}] {self.source_name:<24} "
            f"last={last_val:<10} avg={avg_val:<10} "
            f"trend={trend_symbol}  alerts={self.alert_count}  "
            f"checked={self.last_checked or 'never'}"
        )


class Dashboard:
    """Renders a tabular summary of pipeline health to the terminal."""

    def __init__(self, title: str = "PipeWatch Dashboard") -> None:
        self.title = title

    def build_row(
        self,
        report: PipelineReport,
        stats: Optional[AggregateStats] = None,
        trend: Optional[TrendResult] = None,
    ) -> DashboardRow:
        last_value = None
        last_checked = None
        if report.metric_result and report.metric_result.metric:
            last_value = report.metric_result.metric.value
            ts = report.metric_result.metric.timestamp
            last_checked = ts.strftime("%H:%M:%S") if ts else None

        avg_value = stats.mean if stats else None
        trend_label = trend.direction.value if trend else "insufficient_data"
        alert_count = len(report.active_alerts)

        return DashboardRow(
            source_name=report.source_name,
            status=report.status_label,
            last_value=last_value,
            avg_value=avg_value,
            trend=trend_label,
            alert_count=alert_count,
            last_checked=last_checked,
        )

    def render(self, rows: List[DashboardRow]) -> str:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        header = f"{'=' * 80}\n  {self.title}  —  {now}\n{'=' * 80}"
        if not rows:
            return header + "\n  No pipeline data available.\n"
        lines = [header]
        for row in rows:
            lines.append("  " + row.to_display_line())
        lines.append("=" * 80)
        return "\n".join(lines)

    def print_dashboard(self, rows: List[DashboardRow]) -> None:
        print(self.render(rows))
