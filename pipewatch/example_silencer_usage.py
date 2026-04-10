"""Demonstrates alert silencing during a simulated maintenance window."""

from __future__ import annotations

import time

from pipewatch.alerts import Alert
from pipewatch.silencer import SilenceRule, Silencer


def run_demo() -> None:
    silencer = Silencer()

    # Silence all alerts from the 'warehouse' source for the next 30 minutes.
    maintenance_end = time.time() + 1800
    silencer.add_rule(
        SilenceRule(
            source_name="warehouse",
            alert_name=None,
            expires_at=maintenance_end,
            reason="Scheduled ETL maintenance window",
        )
    )

    # Silence a specific alert on 'api' for 10 minutes.
    silencer.add_rule(
        SilenceRule(
            source_name="api",
            alert_name="high_error_rate",
            expires_at=time.time() + 600,
            reason="Known spike after deploy — investigating",
        )
    )

    incoming_alerts: list[Alert] = [
        Alert(source_name="warehouse", alert_name="row_count_drop",
              message="Row count dropped 40%", value=60.0, threshold=80.0),
        Alert(source_name="api", alert_name="high_error_rate",
              message="Error rate above threshold", value=12.5, threshold=5.0),
        Alert(source_name="api", alert_name="high_latency",
              message="P99 latency elevated", value=850.0, threshold=500.0),
        Alert(source_name="db", alert_name="replication_lag",
              message="Replication lag exceeded limit", value=45.0, threshold=30.0),
    ]

    print(f"Total incoming alerts : {len(incoming_alerts)}")

    actionable = silencer.filter_alerts(incoming_alerts)
    print(f"Actionable alerts     : {len(actionable)}")
    for alert in actionable:
        print(f"  [{alert.source_name}] {alert.alert_name} — {alert.message}")

    print(f"\nActive silence rules  : {len(silencer.active_rules)}")
    for rule in silencer.active_rules:
        remaining = rule.expires_at - time.time()
        print(f"  source={rule.source_name!r} alert={rule.alert_name!r} "
              f"expires_in={remaining:.0f}s reason={rule.reason!r}")

    # Simulate pruning after rules expire.
    removed = silencer.remove_expired(now=time.time() - 1)  # nothing expired yet
    print(f"\nExpired rules pruned  : {removed}")


if __name__ == "__main__":
    run_demo()
