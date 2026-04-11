"""Shows escalation wired together with the Email notifier and throttler."""

from datetime import datetime, timedelta
from typing import List

from pipewatch.alerts import Alert
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.escalation import EscalationManager, EscalationRule
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.notifier import LogNotifier
from pipewatch.throttle import ThrottleRule, Throttler


def _make_result(source: SourceConfig, healthy: bool) -> MetricResult:
    metric = PipelineMetric(
        row_count=0 if not healthy else 1000,
        freshness_seconds=9999 if not healthy else 10,
        error_rate=0.9 if not healthy else 0.0,
    )
    return MetricResult(
        source=source,
        metric=metric,
        healthy=healthy,
        reasons=[] if healthy else ["simulated failure"],
    )


def run_demo() -> None:
    source = SourceConfig(
        name="events_db",
        type="postgres",
        connection_string="postgresql://localhost/events",
        poll_interval_seconds=30,
    )

    escalation_rule = EscalationRule(
        source_name=None,
        alert_name=None,
        escalate_after_seconds=60,
        max_escalations=2,
    )
    throttle_rule = ThrottleRule(
        source_name=None,
        alert_name=None,
        interval_seconds=45,
    )

    manager = EscalationManager(rules=[escalation_rule])
    throttler = Throttler(rules=[throttle_rule])
    notifier = LogNotifier()

    # Simulate a persistent alert over several cycles
    fake_alert = Alert(
        source_name="events_db",
        alert_name="row_count_alert",
        message="row_count below threshold",
    )
    active: List[Alert] = [fake_alert]

    base = datetime(2024, 6, 1, 10, 0, 0)
    for i in range(6):
        now = base + timedelta(seconds=30 * i)
        escalated = manager.evaluate(active, now=now)
        sendable = [a for a in escalated if not throttler.is_throttled(a, now=now)]
        if sendable:
            notifier.send(sendable)
            for a in sendable:
                throttler.record_sent(a, now=now)
            print(f"t+{30*i:>3}s  sent {len(sendable)} escalation(s)")
        else:
            print(f"t+{30*i:>3}s  nothing to send")


if __name__ == "__main__":
    run_demo()
