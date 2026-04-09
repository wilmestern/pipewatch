"""Reporter module for formatting and outputting pipeline health summaries."""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from pipewatch.alerts import Alert
from pipewatch.metrics import MetricResult


@dataclass
class PipelineReport:
    """Represents a snapshot report of pipeline health."""

    timestamp: datetime
    source_name: str
    metric_results: List[MetricResult]
    active_alerts: List[Alert]

    @property
    def is_healthy(self) -> bool:
        return len(self.active_alerts) == 0

    @property
    def status_label(self) -> str:
        return "HEALTHY" if self.is_healthy else "DEGRADED"


class Reporter:
    """Formats and prints pipeline health reports to stdout."""

    def __init__(self, use_color: bool = True):
        self.use_color = use_color

    def _colorize(self, text: str, color_code: str) -> str:
        if not self.use_color:
            return text
        return f"\033[{color_code}m{text}\033[0m"

    def _green(self, text: str) -> str:
        return self._colorize(text, "32")

    def _red(self, text: str) -> str:
        return self._colorize(text, "31")

    def _yellow(self, text: str) -> str:
        return self._colorize(text, "33")

    def format_report(self, report: PipelineReport) -> str:
        lines = []
        ts = report.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        status = (
            self._green(report.status_label)
            if report.is_healthy
            else self._red(report.status_label)
        )
        lines.append(f"[{ts}] Source: {report.source_name}  Status: {status}")

        for result in report.metric_results:
            healthy_str = self._green("OK") if result.healthy else self._red("FAIL")
            lines.append(
                f"  Metric '{result.metric.name}': value={result.metric.value} [{healthy_str}]"
            )

        if report.active_alerts:
            lines.append("  Alerts:")
            for alert in report.active_alerts:
                lines.append(
                    f"    " + self._yellow(f"! {alert.alert_config.name}: {alert.message}")
                )

        return "\n".join(lines)

    def print_report(self, report: PipelineReport) -> None:
        print(self.format_report(report))
