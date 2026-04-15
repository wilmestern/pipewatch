"""Watchdog: detects sources that have stopped reporting metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.checkpoint import CheckpointStore


@dataclass
class WatchdogRule:
    source_name: str
    max_silence_seconds: float

    def is_stale(self, last_seen: datetime, now: Optional[datetime] = None) -> bool:
        """Return True if the source has been silent longer than allowed."""
        now = now or datetime.utcnow()
        return (now - last_seen).total_seconds() > self.max_silence_seconds


@dataclass
class WatchdogAlert:
    source_name: str
    last_seen: datetime
    silence_seconds: float

    def summary(self) -> str:
        return (
            f"[WATCHDOG] '{self.source_name}' has been silent for "
            f"{self.silence_seconds:.1f}s (last seen: {self.last_seen.isoformat()})"
        )


class Watchdog:
    """Monitors sources for reporting gaps using checkpoint data."""

    def __init__(
        self,
        rules: List[WatchdogRule],
        store: CheckpointStore,
    ) -> None:
        self._rules: Dict[str, WatchdogRule] = {r.source_name: r for r in rules}
        self._store = store

    def check(self, now: Optional[datetime] = None) -> List[WatchdogAlert]:
        """Return alerts for any sources that have exceeded their silence threshold."""
        now = now or datetime.utcnow()
        alerts: List[WatchdogAlert] = []

        for source_name, rule in self._rules.items():
            entry = self._store.get(source_name)
            if entry is None:
                continue
            last_seen = entry.last_updated
            silence = (now - last_seen).total_seconds()
            if rule.is_stale(last_seen, now=now):
                alerts.append(
                    WatchdogAlert(
                        source_name=source_name,
                        last_seen=last_seen,
                        silence_seconds=silence,
                    )
                )

        return alerts

    def add_rule(self, rule: WatchdogRule) -> None:
        self._rules[rule.source_name] = rule

    def remove_rule(self, source_name: str) -> None:
        self._rules.pop(source_name, None)
