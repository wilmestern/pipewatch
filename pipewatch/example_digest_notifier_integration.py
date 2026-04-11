"""Example showing DigestBuilder integrated with LogNotifier for periodic reporting."""

from datetime import datetime

from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.digest import DigestBuilder
from pipewatch.history import SourceHistory
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.notifier import LogNotifier
from pipewatch.alerts import Alert


def _make_result(src: SourceConfig, acfg: AlertConfig, healthy: bool, latency: float) -> MetricResult:
    metric = PipelineMetric(
        source_name=src.name,
        latency_ms=latency,
        record_count=50,
        error_count=0 if healthy else 15,
        timestamp=datetime.utcnow(),
    )
    return MetricResult(
        source=src,
        metric=metric,
        alerts=[] if healthy else [acfg],
        collected_at=datetime.utcnow(),
    )


def run_demo() -> None:
    src = SourceConfig(name="billing-pipeline", url="http://billing.internal/metrics", interval=60)
    acfg = AlertConfig(metric="latency_ms", threshold=300.0, operator="gt")

    history = SourceHistory(source_name=src.name)
    for lat in [280, 310, 350, 290, 320]:
        history.record(_make_result(src, acfg, healthy=(lat < acfg.threshold), latency=lat))

    builder = DigestBuilder(histories={src.name: history}, min_samples=3)
    report = builder.build()

    # Log the full digest summary
    print(report.summary())

    # Optionally notify via LogNotifier if any source is unhealthy
    if report.unhealthy_count > 0:
        notifier = LogNotifier()
        fake_alerts = [
            Alert(
                source_name=sd.source_name,
                alert_config=acfg,
                triggered_at=datetime.utcnow(),
                current_value=sd.stats.avg_latency_ms,
            )
            for sd in report.sources
            if not sd.stats.is_healthy
        ]
        notifier.send(fake_alerts)
        print(f"Notified {len(fake_alerts)} unhealthy source(s).")


if __name__ == "__main__":
    run_demo()
