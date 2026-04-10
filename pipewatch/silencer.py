"""Alert silencing (muting) support for pipewatch.

Allows users to suppress alerts for specific sources or alert names
for a defined duration, preventing notification fatigue during
planned maintenance or known degraded states.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerts import Alert


@dataclass
class SilenceRule:
    """A rule that suppresses matching alerts until *expires_at* (epoch seconds)."""

    source_name: Optional[str]  # None means match any source
    alert_name: Optional[str]   # None means match any alert name
    expires_at: float           # epoch seconds
    reason: str = ""

    def is_expired(self, now: Optional[float] = None) -> bool:
        """Return True if this rule has passed its expiry time."""
        now = now if now is not None else time.time()
        return now >= self.expires_at

    def matches(self, alert: Alert) -> bool:
        """Return True when this rule covers *alert*."""
        if self.source_name is not None and alert.source_name != self.source_name:
            return False
        if self.alert_name is not None and alert.alert_name != self.alertn        return True


class Silencer:
    """Manages a collection of silence rules and filters alert lists."""

    def __init__(self) -> None:
        self._rules: list[SilenceRule] = []

    def add_rule(self, rule: SilenceRule) -> None:
        """Register a new silence rule."""
        self._rules.append(rule)

    def remove_expired(self, now: Optional[float] = None) -> int:
        """Prune expired rules and return the count removed."""
        before = len(self._rules)
        self._rules = [r for r in self._rules if not r.is_expired(now)]
        return before - len(self._rules)

    def is_silenced(self, alert: Alert, now: Optional[float] = None) -> bool:
        """Return True if *alert* is covered by at least one active rule."""
        now = now if now is not None else time.time()
        return any(
            not r.is_expired(now) and r.matches(alert)
            for r in self._rules
        )

    def filter_alerts(self, alerts: list[Alert], now: Optional[float] = None) -> list[Alert]:
        """Return only the alerts that are *not* currently silenced."""
        now = now if now is not None else time.time()
        return [a for a in alerts if not self.is_silenced(a, now)]

    @property
    def active_rules(self) -> list[SilenceRule]:
        """Return rules that have not yet expired."""
        now = time.time()
        return [r for r in self._rules if not r.is_expired(now)]
