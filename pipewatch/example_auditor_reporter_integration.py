"""Example showing Auditor integrated with Reporter for annotated reports."""

from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.auditor import Auditor
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.reporter import Reporter


def _make_result(source: SourceConfig, healthy: bool) -> MetricResult:
    metric = PipelineMetric(
        source_name=source.name,
        row_count=800 if healthy else 10,
        latency_ms=50.0,
        collected_at=datetime.now(timezone.utc),
    )
    return MetricResult(metric=metric, healthy=healthy, errors=[])


def run_demo() -> None:
    source = SourceConfig(name="orders_db", query="SELECT COUNT(*) FROM orders", interval_seconds=60)
    alert_cfg = AlertConfig(name="low_rows", min_row_count=100)
    reporter = Reporter()
    auditor = Auditor()

    # First cycle — healthy
    result = _make_result(source, healthy=True)
    auditor.record(source.name, "metric_collected", f"healthy={result.healthy}")
    report = reporter.build(source, alert_cfg, result, active_alerts=[])
    print("Report:", report.status_label)

    # Second cycle — unhealthy, alert fires
    result2 = _make_result(source, healthy=False)
    auditor.record(source.name, "metric_collected", f"healthy={result2.healthy}")
    auditor.record(source.name, "alert_fired", "row_count below min_row_count=100")
    report2 = reporter.build(source, alert_cfg, result2, active_alerts=["low_rows"])
    print("Report:", report2.status_label)

    print("\nAudit trail for", source.name)
    for event in auditor.events_for(source.name):
        print(" ", event.summary())


if __name__ == "__main__":
    run_demo()
