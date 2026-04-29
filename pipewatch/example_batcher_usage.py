"""Example demonstrating Batcher usage in pipewatch."""

from datetime import datetime, timezone

from pipewatch.batcher import Batcher
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric


def _make_result(source_name: str, value: float, threshold: float) -> MetricResult:
    alert_cfg = AlertConfig(name="row_count", threshold=threshold, comparator="gte")
    metric = PipelineMetric(source_name=source_name, value=value)
    healthy = value >= threshold
    return MetricResult(
        source_name=source_name,
        metric=metric,
        is_healthy=healthy,
        alerts=[] if healthy else [alert_cfg],
        collected_at=datetime.now(timezone.utc),
    )


def run_demo() -> None:
    batcher = Batcher(max_size=3)

    sources = ["warehouse", "api_ingest", "warehouse", "api_ingest", "warehouse"]
    values = [500, 20, 300, 150, 10]

    print("Adding results to batcher...")
    for source, val in zip(sources, values):
        result = _make_result(source, val, 100)
        batch = batcher.add(result)
        if batch:
            print(f"  Auto-flushed: {batch.summary()}")
        else:
            print(f"  Buffered {source} (pending: {batcher.pending_count(source)})")

    print("\nFlushing remaining buffers...")
    remaining = batcher.flush_all()
    for batch in remaining:
        print(f"  Manual flush: {batch.summary()}")


if __name__ == "__main__":
    run_demo()
