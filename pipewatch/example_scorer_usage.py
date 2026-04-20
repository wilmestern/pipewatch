"""Example demonstrating the Scorer module.

Shows how to build a ScoreReport from historical metric data
and active alerts, then print per-source health grades.
"""

from datetime import datetime
from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.history import SourceHistory
from pipewatch.scorer import Scorer


def _make_result(source: SourceConfig, healthy: bool) -> MetricResult:
    return MetricResult(
        source=source,
        metric=PipelineMetric(row_count=100 if healthy else 2, latency_ms=50),
        is_healthy=healthy,
        checked_at=datetime.utcnow(),
    )


def run_demo() -> None:
    sources = [
        SourceConfig(name="orders_pipeline", query="SELECT COUNT(*) FROM orders", interval=60),
        SourceConfig(name="inventory_feed", query="SELECT COUNT(*) FROM inventory", interval=120),
    ]

    alert_cfg = AlertConfig(name="low_row_count", threshold=5, operator="lt")

    history = SourceHistory(max_entries=100)

    # Simulate history: orders mostly healthy, inventory struggling
    for healthy in [True, True, True, True, False]:
        history.record(sources[0].name, _make_result(sources[0], healthy))
    for healthy in [True, False, False, False, False]:
        history.record(sources[1].name, _make_result(sources[1], healthy))

    # One active alert on inventory_feed
    active_alerts = {
        "inventory_feed": [
            Alert(
                source_name="inventory_feed",
                config=alert_cfg,
                triggered_at=datetime.utcnow(),
            )
        ]
    }

    scorer = Scorer(history=history, alert_weight=0.2)
    report = scorer.compute(alerts_by_source=active_alerts)

    print(f"Average pipeline health score: {report.average_score:.2f}\n")
    for score in report.scores:
        print(" ", score.summary())


if __name__ == "__main__":
    run_demo()
