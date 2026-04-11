"""Escalation policy: re-notify if an alert remains active beyond a threshold."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.alerts import Alert


@dataclass
class EscalationRule:
    """Defines when and how many times an alert should be escalated."""

    source_name: Optional[str]  # None means apply to all sources
    alert_name: Optional[str]   # None means apply to all alert names
    escalate_after_seconds: int = 300
    max_escalations: int = 3

    def matches(self, alert: Alert) -> bool:
        source_ok = self.source_name is None or self.source_name == alert.source_name
        name_ok = self.alert_name is None or self.alert_name == alert.alert_name
        return source_ok and name_ok


@dataclass
class EscalationState:
    """Tracks escalation history for a single alert."""

    first_seen: datetime
    escalation_count: int = 0
    last_escalated_at: Optional[datetime] = None


class EscalationManager:
    """Determines which active alerts should trigger an escalation notification."""

    def __init__(self, rules: List[EscalationRule]) -> None:
        self._rules = rules
        self._states: Dict[str, EscalationState] = {}

    def _key(self, alert: Alert) -> str:
        return f"{alert.source_name}:{alert.alert_name}"

    def _rule_for(self, alert: Alert) -> Optional[EscalationRule]:
        for rule in self._rules:
            if rule.matches(alert):
                return rule
        return None

    def evaluate(self, active_alerts: List[Alert], now: Optional[datetime] = None) -> List[Alert]:
        """Return alerts that should be escalated right now."""
        now = now or datetime.utcnow()
        to_escalate: List[Alert] = []
        current_keys = set()

        for alert in active_alerts:
            key = self._key(alert)
            current_keys.add(key)
            rule = self._rule_for(alert)
            if rule is None:
                continue

            if key not in self._states:
                self._states[key] = EscalationState(first_seen=now)
                continue

            state = self._states[key]
            if state.escalation_count >= rule.max_escalations:
                continue

            reference = state.last_escalated_at or state.first_seen
            elapsed = (now - reference).total_seconds()
            if elapsed >= rule.escalate_after_seconds:
                to_escalate.append(alert)
                state.escalation_count += 1
                state.last_escalated_at = now

        # Clean up resolved alerts
        for key in list(self._states.keys()):
            if key not in current_keys:
                del self._states[key]

        return to_escalate

    def reset(self, source_name: str, alert_name: str) -> None:
        """Manually clear escalation state for a specific alert."""
        key = f"{source_name}:{alert_name}"
        self._states.pop(key, None)
