"""Example: using Classifier to bucket pipeline results by health category."""
from __future__ import annotations

from pipewatch.classifier import ClassifyRule, Classifier
from pipewatch.config import SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric


def _make_result(name: str, value: float, healthy: bool) -> MetricResult:
    src = SourceConfig(name=name, type="postgres", connection="postgresql://localhost/test")
    metric = PipelineMetric(source_name=name, value=value)
    return MetricResult(source=src, metric=metric, is_healthy=healthy)


def run_demo() -> None:
    rules = [
        ClassifyRule(category="critical", only_unhealthy=True, min_value=80.0),
        ClassifyRule(category="degraded", only_unhealthy=True),
        ClassifyRule(category="nominal"),
    ]
    classifier = Classifier(rules=rules, default_category="uncategorized")

    results = [
        _make_result("payments", 95.0, healthy=False),
        _make_result("users",    40.0, healthy=False),
        _make_result("orders",    5.0, healthy=True),
        _make_result("inventory", 1.0, healthy=True),
    ]

    grouped = classifier.by_category(results)
    for category, items in sorted(grouped.items()):
        print(f"\n=== {category.upper()} ===")
        for cr in items:
            print(" ", cr.summary())


if __name__ == "__main__":
    run_demo()
