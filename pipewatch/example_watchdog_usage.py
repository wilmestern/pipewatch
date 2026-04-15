"""Example: using Watchdog to detect silent pipeline sources."""

from datetime import datetime, timedelta

from pipewatch.checkpoint import CheckpointStore
from pipewatch.watchdog import Watchdog, WatchdogRule


def run_demo() -> None:
    store = CheckpointStore()

    # Simulate sources that have reported at some point
    store.update("orders_pipeline", value=42.0)
    store.update("inventory_pipeline", value=7.0)
    store.update("payments_pipeline", value=99.0)

    now = datetime.utcnow()

    # Manually backdate two sources to simulate silence
    store._entries["orders_pipeline"].last_updated = now - timedelta(seconds=30)
    store._entries["inventory_pipeline"].last_updated = now - timedelta(seconds=200)
    store._entries["payments_pipeline"].last_updated = now - timedelta(seconds=90)

    rules = [
        WatchdogRule(source_name="orders_pipeline", max_silence_seconds=60.0),
        WatchdogRule(source_name="inventory_pipeline", max_silence_seconds=120.0),
        WatchdogRule(source_name="payments_pipeline", max_silence_seconds=60.0),
    ]

    watchdog = Watchdog(rules=rules, store=store)
    alerts = watchdog.check(now=now)

    if not alerts:
        print("All sources reporting normally.")
    else:
        for alert in alerts:
            print(alert.summary())


if __name__ == "__main__":
    run_demo()
