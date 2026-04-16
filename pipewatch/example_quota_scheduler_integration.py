"""Example integrating QuotaManager with Scheduler-style collection."""
from pipewatch.quota import QuotaRule, QuotaManager
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.config import SourceConfig
from datetime import datetime


def _make_result(source_name: str, healthy: bool) -> MetricResult:
    cfg = SourceConfig(name=source_name, type="prometheus", url="http://localhost")
    metric = PipelineMetric(
        source_name=source_name,
        value=1.0 if healthy else 0.0,
        timestamp=datetime.utcnow(),
    )
    return MetricResult(source=cfg, metric=metric, success=True, error=None)


def run_demo() -> None:
    rules = [
        QuotaRule(source_name="db_pipeline", max_runs=2, window_seconds=60),
    ]
    manager = QuotaManager(rules=rules)

    attempts = ["db_pipeline"] * 4

    for source_name in attempts:
        quota_result = manager.check_and_record(source_name)
        if not quota_result.allowed:
            print(f"[QUOTA] Skipping {source_name}: {quota_result.summary()}")
            continue
        result = _make_result(source_name, healthy=True)
        print(f"[COLLECT] {result.source.name} — healthy={result.metric.value}")


if __name__ == "__main__":
    run_demo()
