"""Alert evaluation and dispatch for pipewatch."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from pipewatch.config import AlertConfig
from pipewatch.metrics import PipelineMetric

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Represents a triggered alert."""

    source_name: str
    message: str
    severity: str
    metric_value: float
    triggered_at: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False


class AlertEvaluator:
    """Evaluates metrics against alert rules and dispatches alerts."""

    def __init__(self, alert_config: AlertConfig):
        self.alert_config = alert_config
        self._active_alerts: list[Alert] = []

    def evaluate(self, metric: PipelineMetric) -> Optional[Alert]:
        """Check a metric against the alert config; return an Alert if triggered."""
        triggered = False
        reason_parts: list[str] = []

        if (
            self.alert_config.min_threshold is not None
            and metric.value < self.alert_config.min_threshold
        ):
            triggered = True
            reason_parts.append(
                f"value {metric.value} below minimum {self.alert_config.min_threshold}"
            )

        if (
            self.alert_config.max_threshold is not None
            and metric.value > self.alert_config.max_threshold
        ):
            triggered = True
            reason_parts.append(
                f"value {metric.value} above maximum {self.alert_config.max_threshold}"
            )

        if not triggered:
            return None

        alert = Alert(
            source_name=metric.source_name,
            message=f"[{metric.source_name}] Alert: {'; '.join(reason_parts)}",
            severity=self.alert_config.severity,
            metric_value=metric.value,
        )
        self._active_alerts.append(alert)
        logger.warning(alert.message)
        return alert

    def active_alerts(self) -> list[Alert]:
        """Return all unresolved alerts."""
        return [a for a in self._active_alerts if not a.resolved]

    def resolve_all(self) -> None:
        """Mark all active alerts as resolved."""
        for alert in self._active_alerts:
            alert.resolved = True
