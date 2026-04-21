"""Example demonstrating Auditor usage in a pipeline monitoring loop."""

from __future__ import annotations

from pipewatch.auditor import Auditor


def run_demo() -> None:
    auditor = Auditor(max_events=500)

    # Simulate a collection cycle for two sources
    sources = ["postgres_primary", "kafka_ingest"]
    for source in sources:
        auditor.record(source, "metric_collected", "row_count=1024, latency_ms=42")

    # Simulate an alert firing on one source
    auditor.record("kafka_ingest", "alert_fired", "row_count dropped below threshold (min=500)")

    # Simulate resolution after the next healthy collection
    auditor.record("kafka_ingest", "metric_collected", "row_count=780, latency_ms=38")
    auditor.record("kafka_ingest", "alert_resolved", "row_count back above threshold")

    print("=== All audit events ===")
    for event in auditor.all_events():
        print(" ", event.summary())

    print("\n=== Events for kafka_ingest ===")
    for event in auditor.events_for("kafka_ingest"):
        print(" ", event.summary())

    print("\n=== Alert-fired events ===")
    for event in auditor.events_by_type("alert_fired"):
        print(" ", event.summary())

    latest = auditor.latest("postgres_primary")
    print(f"\nLatest event for postgres_primary: {latest.summary() if latest else 'none'}")


if __name__ == "__main__":
    run_demo()
