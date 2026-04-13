"""Correlator: detect co-occurring alert patterns across multiple sources."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from pipewatch.alerts import Alert


@dataclass
class CorrelationRule:
    """Defines a pattern of alerts that should be treated as correlated."""
    name: str
    source_names: List[str]
    alert_names: List[str]
    window_seconds: int = 60

    def key(self) -> str:
        sources = ",".join(sorted(self.source_names))
        alerts = ",".join(sorted(self.alert_names))
        return f"{self.name}|{sources}|{alerts}"


@dataclass
class CorrelationMatch:
    """Represents a detected correlation between alerts."""
    rule_name: str
    matched_alerts: List[Alert]
    detected_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def summary(self) -> str:
        sources = ", ".join(a.source_name for a in self.matched_alerts)
        return f"[{self.rule_name}] correlated alerts from: {sources}"


class Correlator:
    """Evaluates incoming alerts against correlation rules within time windows."""

    def __init__(self, rules: Optional[List[CorrelationRule]] = None) -> None:
        self._rules: List[CorrelationRule] = rules or []
        # Maps rule key -> list of (timestamp, Alert)
        self._buffer: Dict[str, List[Tuple[datetime, Alert]]] = {}

    def add_rule(self, rule: CorrelationRule) -> None:
        self._rules.append(rule)

    def _prune(self, rule: CorrelationRule, now: datetime) -> None:
        key = rule.key()
        cutoff = now - timedelta(seconds=rule.window_seconds)
        self._buffer[key] = [
            (ts, alert)
            for ts, alert in self._buffer.get(key, [])
            if ts >= cutoff
        ]

    def evaluate(self, alert: Alert, now: Optional[datetime] = None) -> List[CorrelationMatch]:
        """Record an alert and return any newly matched correlations."""
        if now is None:
            now = datetime.utcnow()

        matches: List[CorrelationMatch] = []

        for rule in self._rules:
            if alert.source_name not in rule.source_names:
                continue
            if alert.alert_name not in rule.alert_names:
                continue

            key = rule.key()
            self._prune(rule, now)
            self._buffer.setdefault(key, []).append((now, alert))

            buffered_alerts = [a for _, a in self._buffer[key]]
            matched_sources = {a.source_name for a in buffered_alerts}
            matched_alert_names = {a.alert_name for a in buffered_alerts}

            if (
                set(rule.source_names) <= matched_sources
                and set(rule.alert_names) <= matched_alert_names
            ):
                matches.append(CorrelationMatch(
                    rule_name=rule.name,
                    matched_alerts=list(buffered_alerts),
                    detected_at=now,
                ))
                self._buffer[key] = []  # reset after match

        return matches
