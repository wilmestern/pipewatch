"""Example showing SnapshotManager usage for change detection."""
from datetime import datetime, timezone

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult
from pipewatch.snapshot import SnapshotManager


def _make_result(
    source: SourceConfig, alert: AlertConfig, healthy: bool
) -> MetricResult:
    return MetricResult(
        source=source,
        alert=alert,
        value=500.0 if healthy else 5.0,
        is_healthy=healthy,
        collected_at=datetime.now(tz=timezone.utc),
    )


def run_demo() -> None:
    db_source = SourceConfig(
        name="orders_db",
        type="postgres",
        connection="postgresql://localhost/orders",
    )
    api_source = SourceConfig(
        name="payments_api",
        type="http",
        connection="https://api.example.com/health",
    )
    row_alert = AlertConfig(
        name="row_count", metric="row_count", threshold=100, operator="gte"
    )

    manager = SnapshotManager()

    # --- Cycle 1: both sources healthy ---
    manager.capture({
        "orders_db": _make_result(db_source, row_alert, True),
        "payments_api": _make_result(api_source, row_alert, True),
    })
    snap1 = manager.latest()
    print("Snapshot 1:", snap1.summary())

    # --- Cycle 2: payments_api degrades, new source appears ---
    manager.capture({
        "orders_db": _make_result(db_source, row_alert, True),
        "payments_api": _make_result(api_source, row_alert, False),
        "analytics_db": _make_result(db_source, row_alert, True),
    })
    snap2 = manager.latest()
    print("Snapshot 2:", snap2.summary())

    diff = manager.diff()
    if diff and diff.has_changes:
        print("Changes detected:", diff.summary())
    else:
        print("No changes between snapshots.")

    # --- Cycle 3: payments_api recovers ---
    manager.capture({
        "orders_db": _make_result(db_source, row_alert, True),
        "payments_api": _make_result(api_source, row_alert, True),
        "analytics_db": _make_result(db_source, row_alert, True),
    })
    diff2 = manager.diff()
    print("After recovery:", diff2.summary() if diff2 else "no diff")
    print(f"Total snapshots captured: {len(manager.history())}")


if __name__ == "__main__":
    run_demo()
