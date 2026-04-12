"""Example: using Router to direct alerts to different backends."""

from __future__ import annotations

from datetime import datetime

from pipewatch.alerts import Alert
from pipewatch.notifier import LogNotifier, EmailNotifier
from pipewatch.router import RouteRule, Router


def run_demo() -> None:
    # Build sample alerts
    db_alert = Alert(
        source_name="database",
        alert_name="row_lag",
        message="Row lag exceeds threshold",
        triggered_at=datetime.utcnow(),
    )
    api_alert = Alert(
        source_name="api",
        alert_name="error_rate",
        message="Error rate above 5%",
        triggered_at=datetime.utcnow(),
    )

    # Configure routing rules
    rules = [
        # Critical DB alerts go to email
        RouteRule(backend_names=["email"], source_name="database"),
        # Everything else goes to log
        RouteRule(backend_names=["log"]),
    ]

    router = Router(rules=rules)

    # Register backends
    router.register("log", LogNotifier())
    router.register(
        "email",
        EmailNotifier(
            smtp_host="localhost",
            smtp_port=25,
            sender="pipewatch@example.com",
            recipients=["oncall@example.com"],
        ),
    )

    print("Routing alerts...")
    sent = router.route([db_alert, api_alert])

    for backend_name, routed_alerts in sent.items():
        print(f"  [{backend_name}] received {len(routed_alerts)} alert(s)")
        for a in routed_alerts:
            print(f"    - {a.source_name}/{a.alert_name}: {a.message}")


if __name__ == "__main__":
    run_demo()
