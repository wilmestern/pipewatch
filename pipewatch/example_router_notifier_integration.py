"""Example: integrating Router with Scheduler for end-to-end alert routing."""

from __future__ import annotations

from datetime import datetime

from pipewatch.alerts import Alert, AlertEvaluator
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricsCollector
from pipewatch.notifier import LogNotifier
from pipewatch.router import RouteRule, Router


def _make_result(source: str, value: float):
    config = SourceConfig(name=source, type="prometheus", url="http://localhost:9090", metric="rows_processed")
    collector = MetricsCollector(config)
    return collector.record(value)


def run_demo() -> None:
    alert_cfg = AlertConfig(name="low_throughput", threshold=100.0, comparator="lt", severity="critical")
    source_cfg = SourceConfig(
        name="warehouse",
        type="prometheus",
        url="http://localhost:9090",
        metric="rows_processed",
        alerts=[alert_cfg],
    )

    evaluator = AlertEvaluator(source_cfg)

    # Simulate an unhealthy metric
    result = _make_result("warehouse", 42.0)
    active = evaluator.evaluate(result)

    print(f"Active alerts: {len(active)}")
    for a in active:
        print(f"  {a.source_name}/{a.alert_name} — {a.message}")

    # Set up router
    rules = [
        RouteRule(backend_names=["log"], source_name="warehouse"),
    ]
    router = Router(rules=rules, default_backend="log")
    router.register("log", LogNotifier())

    print("\nRouting alerts via Router...")
    sent = router.route(active)
    for backend, routed in sent.items():
        print(f"  [{backend}] dispatched {len(routed)} alert(s)")


if __name__ == "__main__":
    run_demo()
