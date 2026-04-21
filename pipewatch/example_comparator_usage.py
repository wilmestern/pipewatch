"""Example: using Comparator to detect metric regressions."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.history import SourceHistory
from pipewatch.comparator import Comparator


def _make_result(cfg: SourceConfig, acfg: AlertConfig, value: float) -> MetricResult:
    metric = PipelineMetric(
        source_name=cfg.name,
        metric_name=acfg.metric,
        value=value,
        timestamp=datetime.now(timezone.utc),
    )
    return MetricResult(source_config=cfg, metric=metric, is_healthy=value <= acfg.threshold)


def run_demo() -> None:
    source = SourceConfig(name="orders_db", source_type="postgres", connection_string="postgresql://localhost/orders")
    alert = AlertConfig(name="latency", metric="latency_ms", threshold=300.0, operator="gt")

    history = SourceHistory()

    # Simulate a stable baseline followed by a regression
    baseline_values = [120.0, 115.0, 125.0, 118.0, 122.0]
    for v in baseline_values:
        history.record(source.name, _make_result(source, alert, v))

    # Record a regression spike
    history.record(source.name, _make_result(source, alert, 310.0))

    comparator = Comparator(history=history, window_size=5)
    result = comparator.compare(source.name)

    if result is None:
        print("Not enough data to compare.")
        return

    print("Comparison result:")
    print(f"  {result.summary()}")
    if not result.improved:
        print(f"  ⚠ Regression detected for '{source.name}'!")
    else:
        print(f"  ✓ Metric is within or better than historical baseline.")


if __name__ == "__main__":
    run_demo()
