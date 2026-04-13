"""labeler.py — Assigns severity labels to alerts based on configurable rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from pipewatch.alerts import Alert


class Severity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class LabelRule:
    severity: Severity
    source_name: Optional[str] = None  # None means match any source
    alert_name: Optional[str] = None   # None means match any alert name

    def matches(self, alert: Alert) -> bool:
        if self.source_name is not None and alert.source_name != self.source_name:
            return False
        if self.alert_name is not None and alert.alert_name != self.alert_name:
            return False
        return True


@dataclass
class LabeledAlert:
    alert: Alert
    severity: Severity

    @property
    def summary(self) -> str:
        return f"[{self.severity.value.upper()}] {self.alert.source_name}/{self.alert.alert_name}"


@dataclass
class Labeler:
    rules: List[LabelRule] = field(default_factory=list)
    default_severity: Severity = Severity.MEDIUM

    def add_rule(self, rule: LabelRule) -> None:
        self.rules.append(rule)

    def label(self, alert: Alert) -> LabeledAlert:
        for rule in self.rules:
            if rule.matches(alert):
                return LabeledAlert(alert=alert, severity=rule.severity)
        return LabeledAlert(alert=alert, severity=self.default_severity)

    def label_all(self, alerts: List[Alert]) -> List[LabeledAlert]:
        return [self.label(a) for a in alerts]

    def filter_by_severity(
        self, labeled: List[LabeledAlert], minimum: Severity
    ) -> List[LabeledAlert]:
        order = [Severity.INFO, Severity.LOW, Severity.MEDIUM, Severity.HIGH, Severity.CRITICAL]
        min_index = order.index(minimum)
        return [la for la in labeled if order.index(la.severity) >= min_index]
