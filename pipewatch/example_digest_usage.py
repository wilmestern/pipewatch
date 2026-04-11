"""Example demonstrating DigestBuilder producing a periodic summary report."""

from datetime import datetime

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.digest import DigestBuilder
from pipewatch.history import SourceHistory
from pipewatch.metrics import MetricResult, PipelineMetric


def _make_result(
    source_config: SourceConfig,
    alert_config: AlertConfig,
    healthy: bool,
    latency: float,
) -> MetricResult:
    metric = PipelineMetric(
        source_name=source_config.name,
        latency_ms=latency,
        record_count=100,
        error_count=0 if healthy else 20,
        timestamp=datetime.utcnow(),
    )
    return MetricResult(
        source=source_config,
        metric=metric,
        alerts=[] if healthy else [alert_config],
        collected_at=datetime.utcnow(),
    )


def run_demo() -> None:
    sources = [
        SourceConfig(name="orders-pipeline", url="http://orders.internal/metrics", interval=60),
        SourceConfig(name="payments-pipeline", url="http://payments.internal/metrics", interval=60),
        SourceConfig(name="inventory-pipeline", url="http://inventory.internal/metrics", interval=60),
    ]
    alert_cfg = AlertConfig(metric="latency_ms", threshold=400.0, operator="gt")

    histories: dict = {}
    latencies = {
        "orders-pipeline": [120, 130, 125, 140, 135],
        "payments-pipeline": [450, 480, 510, 490, 520],
        "inventory-pipeline": [80, 85, 78, 90, 88],
    }

    for src in sources:
        h = SourceHistory(source_name=src.name)
        for lat in latencies[src.name]:
            healthy = lat < alert_cfg.threshold
            h.record(_make_result(src, alert_cfg, healthy=healthy, latency=lat))
        histories[src.name] = h

    builder = DigestBuilder(histories=histories, min_samples=3)
    report = builder.build()
    print(report.summary())


if __name__ == "__main__":
    run_demo()
