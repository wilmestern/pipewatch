"""Example demonstrating the Dashboard with mock pipeline data."""

from datetime import datetime

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import PipelineMetric, MetricResult
from pipewatch.alerts import Alert
from pipewatch.reporter import PipelineReport
from pipewatch.aggregator import AggregateStats
from pipewatch.trend import TrendResult, TrendDirection
from pipewatch.dashboard import Dashboard


def run_demo() -> None:
    dashboard = Dashboard(title="PipeWatch Live Dashboard")

    sources = [
        ("orders_db", 320.0, True, 310.5, TrendDirection.RISING, 0),
        ("events_kafka", 5.0, False, 88.2, TrendDirection.FALLING, 2),
        ("user_api", 150.0, True, 148.0, TrendDirection.STABLE, 0),
    ]

    rows = []
    for name, value, healthy, avg, direction, n_alerts in sources:
        metric = PipelineMetric(source=name, value=value, timestamp=datetime.utcnow())
        result = MetricResult(success=True, metric=metric)

        alerts = []
        for i in range(n_alerts):
            alerts.append(
                Alert(
                    source=name,
                    rule="value < threshold",
                    message=f"Alert {i + 1} for {name}",
                    triggered_at=datetime.utcnow(),
                )
            )

        report = PipelineReport(
            source_name=name,
            metric_result=result,
            active_alerts=alerts,
        )

        stats = AggregateStats(
            mean=avg,
            minimum=avg * 0.8,
            maximum=avg * 1.2,
            count=20,
            healthy_count=18 if healthy else 10,
            unhealthy_count=2 if healthy else 10,
        )

        trend = TrendResult(direction=direction, slope=0.5, sample_count=10)
        row = dashboard.build_row(report, stats=stats, trend=trend)
        rows.append(row)

    dashboard.print_dashboard(rows)


if __name__ == "__main__":
    run_demo()
