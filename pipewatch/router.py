"""Alert routing: direct alerts to specific notifier backends based on rules."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerts import Alert
from pipewatch.notifier import NotificationBackend


@dataclass
class RouteRule:
    """Maps a filter pattern to a list of notifier backend names."""

    backend_names: List[str]
    source_name: Optional[str] = None  # None means match all sources
    alert_name: Optional[str] = None   # None means match all alert names

    def matches(self, alert: Alert) -> bool:
        """Return True if this rule applies to the given alert."""
        if self.source_name is not None and alert.source_name != self.source_name:
            return False
        if self.alert_name is not None and alert.alert_name != self.alert_name:
            return False
        return True


@dataclass
class Router:
    """Routes alerts to the appropriate notification backends."""

    rules: List[RouteRule]
    backends: dict = field(default_factory=dict)  # name -> NotificationBackend
    default_backend: Optional[str] = None

    def register(self, name: str, backend: NotificationBackend) -> None:
        """Register a named notification backend."""
        self.backends[name] = backend

    def route(self, alerts: List[Alert]) -> dict:
        """Route each alert to matching backends.

        Returns a dict mapping backend_name -> list of alerts sent.
        """
        sent: dict = {name: [] for name in self.backends}

        for alert in alerts:
            matched_backends = self._resolve_backends(alert)
            for name in matched_backends:
                backend = self.backends.get(name)
                if backend is not None:
                    backend.send([alert])
                    sent[name].append(alert)

        return sent

    def _resolve_backends(self, alert: Alert) -> List[str]:
        """Return backend names for the first matching rule, or default."""
        for rule in self.rules:
            if rule.matches(alert):
                return rule.backend_names
        if self.default_backend is not None:
            return [self.default_backend]
        return []
