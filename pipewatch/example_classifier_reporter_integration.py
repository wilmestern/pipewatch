"""Integration example: Classifier + Reporter to display categorised results."""
from __future__ import annotations

from pipewatch.classifier import ClassifyRule, Classifier
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.reporter import Reporter


def _make_result(name: str, value: float, healthy: bool) -> MetricResult:
    src = SourceConfig(name=name, type="postgres", connection="postgresql://localhost/test")
    metric = PipelineMetric(source_name=name, value=value)
    return MetricResult(source=src, metric=metric, is_healthy=healthy)


def run_demo() -> None:
    alert_cfg = AlertConfig(name="high_latency", threshold=75.0, metric="latency_ms")

    rules = [
        ClassifyRule(category="critical", only_unhealthy=True, min_value=80.0),
        ClassifyRule(category="degraded", only_unhealthy=True),
        ClassifyRule(category="nominal"),
    ]
    classifier = Classifier(rules=rules)

    results = [
        _make_result("payments", 92.0, healthy=False),
        _make_result("users",    55.0, healthy=False),
        _make_result("orders",    3.0, healthy=True),
    ]

    reporter = Reporter()
    grouped = classifier.by_category(results)

    for category, items in sorted(grouped.items()):
        print(f"\n--- Category: {category} ---")
        for cr in items:
            alerts = [] if cr.result.is_healthy else [
                Alert(source=cr.result.source, config=alert_cfg,
                      message=f"{alert_cfg.name} triggered")
            ]
            report = reporter.build(result=cr.result, alerts=alerts)
            print(" ", reporter.format(report))


if __name__ == "__main__":
    run_demo()
