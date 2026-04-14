"""Example demonstrating the Annotator feature."""
from __future__ import annotations

from pipewatch.annotator import Annotator
from pipewatch.config import SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric


def _make_result(source_name: str, healthy: bool) -> MetricResult:
    metric = PipelineMetric(
        row_count=500 if healthy else 0,
        latency_seconds=0.2 if healthy else 9.9,
        error_rate=0.0 if healthy else 0.9,
    )
    return MetricResult(source_name=source_name, metric=metric, success=healthy)


def run_demo() -> None:
    annotator = Annotator()

    # Operators add notes during an incident
    annotator.add("payments_db", "High latency observed — investigating replication lag.", "alice")
    annotator.add("payments_db", "Confirmed: replica fell behind; failover in progress.", "bob")
    annotator.add("kafka_ingest", "Throughput normal; no action needed.", "carol")

    results = [
        _make_result("payments_db", healthy=False),
        _make_result("kafka_ingest", healthy=True),
        _make_result("user_events", healthy=True),
    ]

    print("=== Annotated Pipeline Results ===")
    for result in results:
        ar = annotator.annotate_result(result)
        status = "OK" if result.success else "FAIL"
        print(f"\nSource : {result.source_name} [{status}]")
        if ar.has_annotations():
            for ann in ar.annotations:
                print(f"  Note : {ann.summary()}")
        else:
            print("  Note : (no annotations)")

    # Clean up resolved source
    removed = annotator.clear("payments_db")
    print(f"\nCleared {removed} annotation(s) for 'payments_db' after incident resolved.")


if __name__ == "__main__":
    run_demo()
