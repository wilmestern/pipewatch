"""Shows Annotator integrated with Reporter to display notes in reports."""
from __future__ import annotations

from pipewatch.annotator import Annotator
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.reporter import Reporter


def _make_result(source_name: str, healthy: bool) -> MetricResult:
    metric = PipelineMetric(
        row_count=200 if healthy else 0,
        latency_seconds=0.5 if healthy else 5.0,
        error_rate=0.0 if healthy else 0.4,
    )
    return MetricResult(source_name=source_name, metric=metric, success=healthy)


def run_demo() -> None:
    source_cfg = SourceConfig(
        name="orders_db",
        type="postgres",
        connection_string="postgresql://localhost/orders",
    )
    alert_cfg = AlertConfig(
        latency_threshold_seconds=2.0,
        error_rate_threshold=0.1,
        min_row_count=50,
    )

    annotator = Annotator()
    annotator.add("orders_db", "Investigating slow queries post-deploy.", "devops-team")

    reporter = Reporter(source_cfg, alert_cfg)
    result = _make_result("orders_db", healthy=False)
    report = reporter.build_report(result, active_alerts=[])

    ar = annotator.annotate_result(result)

    print("=== Pipeline Report with Annotations ===")
    print(f"Source  : {report.source_name}")
    print(f"Status  : {report.status_label()}")
    print(f"Healthy : {report.is_healthy()}")
    print()
    if ar.has_annotations():
        print("Operator Notes:")
        for ann in ar.annotations:
            print(f"  - {ann.summary()}")
    else:
        print("Operator Notes: none")


if __name__ == "__main__":
    run_demo()
