"""Alert throttling to prevent notification floods."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple


@dataclass
class ThrottleRule:
    """Defines throttling behaviour for a source/alert combination."""

    source_name: str
    alert_name: str
    min_interval_seconds: int = 300  # 5 minutes default

    def key(self) -> Tuple[str, str]:
        return (self.source_name, self.alert_name)


@dataclass
class Throttler:
    """Tracks last-notification times and suppresses repeated alerts."""

    rules: list[ThrottleRule] = field(default_factory=list)
    _last_sent: Dict[Tuple[str, str], datetime] = field(
        default_factory=dict, init=False, repr=False
    )

    def _interval_for(self, source_name: str, alert_name: str) -> int:
        """Return the configured interval (seconds) for this pair, or 0 if none."""
        for rule in self.rules:
            if rule.source_name == source_name and rule.alert_name == alert_name:
                return rule.min_interval_seconds
        return 0

    def is_throttled(
        self,
        source_name: str,
        alert_name: str,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return True if this alert should be suppressed right now."""
        interval = self._interval_for(source_name, alert_name)
        if interval <= 0:
            return False
        key = (source_name, alert_name)
        last = self._last_sent.get(key)
        if last is None:
            return False
        effective_now = now or datetime.utcnow()
        return (effective_now - last) < timedelta(seconds=interval)

    def record_sent(
        self,
        source_name: str,
        alert_name: str,
        now: Optional[datetime] = None,
    ) -> None:
        """Mark an alert as sent at the given time."""
        key = (source_name, alert_name)
        self._last_sent[key] = now or datetime.utcnow()

    def filter_alerts(self, alerts: list, now: Optional[datetime] = None) -> list:
        """Return only the alerts that are not currently throttled."""
        allowed = []
        for alert in alerts:
            if not self.is_throttled(alert.source_name, alert.name, now=now):
                allowed.append(alert)
        return allowed
