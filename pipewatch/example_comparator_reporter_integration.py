"""Example: integrating Comparator with Reporter to surface regressions."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.history import SourceHistory
from pipewatch.comparator import Comparator
from pipewatch.reporter import Reporter


def _make_result(cfg: SourceConfig, acfg: AlertConfig, value: float) -> MetricResult:
    metric = PipelineMetric(
        source_name=cfg.name,
        metric_name=acfg.metric,
        value=value,
        timestamp=datetime.now(timezone.utc),
    )
    return MetricResult(source_config=cfg, metric=metric, is_healthy=value <= acfg.threshold)


def run_demo() -> None:
    sources = [
        SourceConfig(name="orders_db", source_type="postgres", connection_string="postgresql://localhost/orders"),
        SourceConfig(name="payments_api", source_type="http", connection_string="https://payments.internal/health"),
    ]
    alert_cfg = AlertConfig(name="latency", metric="latency_ms", threshold=300.0, operator="gt")

    history = SourceHistory()
    reporter = Reporter()

    # Populate history with stable baselines
    for src in sources:
        for v in [100.0, 110.0, 105.0, 108.0, 102.0]:
            history.record(src.name, _make_result(src, alert_cfg, v))

    # Simulate a spike on orders_db only
    history.record(sources[0].name, _make_result(sources[0], alert_cfg, 450.0))
    history.record(sources[1].name, _make_result(sources[1], alert_cfg, 101.0))

    comparator = Comparator(history=history, window_size=5)
    comparisons = comparator.compare_all()

    print("=== Comparator + Reporter Integration ===")
    for cmp in comparisons:
        latest = history.latest(cmp.source_name)
        if latest is None:
            continue
        report = reporter.build(latest.result, alerts=[])
        status = report.status_label()
        regression = "⚠ REGRESSION" if not cmp.improved else "✓ OK"
        print(f"[{status}] {cmp.summary()}  {regression}")


if __name__ == "__main__":
    run_demo()
