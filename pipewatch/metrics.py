"""Metrics collection and evaluation for pipeline health monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.config import SourceConfig


@dataclass
class PipelineMetric:
    """Represents a single collected metric from a pipeline source."""

    source_name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    labels: dict = field(default_factory=dict)

    def is_healthy(self, config: SourceConfig) -> bool:
        """Evaluate whether the metric value is within healthy thresholds."""
        if config.min_threshold is not None and self.value < config.min_threshold:
            return False
        if config.max_threshold is not None and self.value > config.max_threshold:
            return False
        return True


@dataclass
class MetricResult:
    """Result of a metric collection attempt."""

    metric: Optional[PipelineMetric]
    success: bool
    error: Optional[str] = None


class MetricsCollector:
    """Collects and evaluates metrics from configured pipeline sources."""

    def __init__(self, source_config: SourceConfig):
        self.source_config = source_config
        self._history: list[PipelineMetric] = []

    def record(self, value: float, labels: Optional[dict] = None) -> MetricResult:
        """Record a new metric value and return the result."""
        try:
            metric = PipelineMetric(
                source_name=self.source_config.name,
                value=value,
                labels=labels or {},
            )
            self._history.append(metric)
            return MetricResult(metric=metric, success=True)
        except Exception as exc:  # pragma: no cover
            return MetricResult(metric=None, success=False, error=str(exc))

    def latest(self) -> Optional[PipelineMetric]:
        """Return the most recently recorded metric."""
        return self._history[-1] if self._history else None

    def history(self, limit: int = 100) -> list[PipelineMetric]:
        """Return recent metric history up to *limit* entries."""
        return self._history[-limit:]

    def clear_history(self) -> None:
        """Clear all recorded metrics."""
        self._history.clear()
