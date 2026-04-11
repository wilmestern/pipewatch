"""Demonstrates escalation policy integrated with the alert evaluator."""

from datetime import datetime, timedelta

from pipewatch.alerts import Alert, AlertEvaluator
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.escalation import EscalationManager, EscalationRule
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.notifier import LogNotifier


def _make_unhealthy_result(source: SourceConfig) -> MetricResult:
    return MetricResult(
        source=source,
        metric=PipelineMetric(row_count=0, freshness_seconds=9999, error_rate=0.5),
        healthy=False,
        reasons=["row_count below threshold"],
    )


def run_demo() -> None:
    source = SourceConfig(
        name="warehouse",
        type="postgres",
        connection_string="postgresql://localhost/dw",
        poll_interval_seconds=60,
    )
    alert_cfg = AlertConfig(
        name="row_count_alert",
        min_row_count=100,
        max_freshness_seconds=300,
        max_error_rate=0.05,
    )

    rule = EscalationRule(
        source_name=None,
        alert_name=None,
        escalate_after_seconds=120,
        max_escalations=3,
    )
    manager = EscalationManager(rules=[rule])
    notifier = LogNotifier()

    evaluator = AlertEvaluator(alert_configs=[alert_cfg])
    result = _make_unhealthy_result(source)

    base_time = datetime(2024, 6, 1, 9, 0, 0)
    active: list[Alert] = evaluator.evaluate(result)
    print(f"Active alerts: {len(active)}")

    for minute in [0, 1, 3, 5, 8]:
        now = base_time + timedelta(minutes=minute)
        escalated = manager.evaluate(active, now=now)
        if escalated:
            print(f"[{now.strftime('%H:%M')}] Escalating {len(escalated)} alert(s)")
            notifier.send(escalated)
        else:
            print(f"[{now.strftime('%H:%M')}] No escalation needed")


if __name__ == "__main__":
    run_demo()
