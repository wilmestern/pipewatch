"""Suppressor: conditionally suppress alerts based on time-of-day or day-of-week windows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import List, Optional

from pipewatch.alerts import Alert


@dataclass
class SuppressionWindow:
    """Defines a time window during which alerts are suppressed."""

    source_name: Optional[str]  # None means apply to all sources
    alert_name: Optional[str]   # None means apply to all alert names
    days: List[int] = field(default_factory=list)  # 0=Monday … 6=Sunday; empty = all days
    start_time: time = time(0, 0)
    end_time: time = time(23, 59, 59)

    def matches(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        """Return True if this window covers *alert* at *now*."""
        if now is None:
            now = datetime.utcnow()

        if self.source_name is not None and self.source_name != alert.source_name:
            return False
        if self.alert_name is not None and self.alert_name != alert.alert_name:
            return False
        if self.days and now.weekday() not in self.days:
            return False

        current = now.time().replace(microsecond=0)
        return self.start_time <= current <= self.end_time


@dataclass
class SuppressResult:
    alert: Alert
    suppressed: bool
    window: Optional[SuppressionWindow] = None

    @property
    def summary(self) -> str:
        if self.suppressed and self.window:
            return (
                f"[SUPPRESSED] {self.alert.source_name}/{self.alert.alert_name} "
                f"matched window (days={self.window.days}, "
                f"{self.window.start_time}–{self.window.end_time})"
            )
        return f"[ALLOWED] {self.alert.source_name}/{self.alert.alert_name}"


class Suppressor:
    """Evaluates alerts against registered suppression windows."""

    def __init__(self, windows: Optional[List[SuppressionWindow]] = None) -> None:
        self._windows: List[SuppressionWindow] = windows or []

    def add_window(self, window: SuppressionWindow) -> None:
        self._windows.append(window)

    def evaluate(self, alert: Alert, now: Optional[datetime] = None) -> SuppressResult:
        """Return a SuppressResult indicating whether *alert* is suppressed."""
        for window in self._windows:
            if window.matches(alert, now=now):
                return SuppressResult(alert=alert, suppressed=True, window=window)
        return SuppressResult(alert=alert, suppressed=False)

    def filter(self, alerts: List[Alert], now: Optional[datetime] = None) -> List[Alert]:
        """Return only the alerts that are *not* suppressed."""
        return [
            a for a in alerts if not self.evaluate(a, now=now).suppressed
        ]
