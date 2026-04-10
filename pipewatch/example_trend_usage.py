"""Example demonstrating trend analysis with SourceHistory."""
from datetime import datetime
from pipewatch.metrics import PipelineMetric, MetricResult
from pipewatch.history import SourceHistory
from pipewatch.trend import TrendAnalyzer


def _make_result(source: str, healthy: bool, latency: float) -> MetricResult:
    metric = PipelineMetric(
        source_name=source,
        row_count=500,
        latency_seconds=latency,
        error_count=0 if healthy else 10,
        collected_at=datetime.utcnow(),
    )
    return MetricResult(source_name=source, metric=metric, healthy=healthy, errors=[])


def run_demo() -> None:
    history = SourceHistory(max_entries=50)
    analyzer = TrendAnalyzer(history, window=8)

    # Simulate a source that starts healthy then degrades
    source = "orders_db"
    print(f"Recording metrics for '{source}'...")

    healthy_runs = [True, True, True, True]
    degraded_runs = [False, False, True, False]

    for i, healthy in enumerate(healthy_runs + degraded_runs):
        latency = 0.5 + i * 0.15
        result = _make_result(source, healthy, latency)
        history.record(result)

    result = analyzer.analyze(source)
    print(result.summary)
    print(f"  Uptime (all-time): {history.uptime(source):.1%}")
    print(f"  Error rate (last 8): {history.error_rate(source, window=8):.1%}")
    avg = history.average_latency(source, window=8)
    if avg is not None:
        print(f"  Avg latency (last 8): {avg:.2f}s")

    # Simulate a stable source
    stable_source = "payments_api"
    for i in range(6):
        history.record(_make_result(stable_source, True, latency=0.3))
    stable_result = analyzer.analyze(stable_source)
    print(f"\n{stable_result.summary}")


if __name__ == "__main__":
    run_demo()
