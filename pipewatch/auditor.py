"""Audit log for pipeline events — records state changes and alert transitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class AuditEvent:
    source_name: str
    event_type: str  # e.g. "alert_fired", "alert_resolved", "metric_collected"
    detail: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def summary(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"[{ts}] {self.event_type} | {self.source_name}: {self.detail}"


class Auditor:
    """Records and retrieves audit events for pipeline sources."""

    def __init__(self, max_events: int = 1000) -> None:
        if max_events <= 0:
            raise ValueError("max_events must be a positive integer")
        self._max_events = max_events
        self._events: List[AuditEvent] = []

    def record(self, source_name: str, event_type: str, detail: str) -> AuditEvent:
        """Record a new audit event."""
        event = AuditEvent(source_name=source_name, event_type=event_type, detail=detail)
        self._events.append(event)
        if len(self._events) > self._max_events:
            self._events = self._events[-self._max_events :]
        return event

    def events_for(self, source_name: str) -> List[AuditEvent]:
        """Return all events for a given source, oldest first."""
        return [e for e in self._events if e.source_name == source_name]

    def events_by_type(self, event_type: str) -> List[AuditEvent]:
        """Return all events of a given type."""
        return [e for e in self._events if e.event_type == event_type]

    def latest(self, source_name: str) -> Optional[AuditEvent]:
        """Return the most recent event for a source, or None."""
        matches = self.events_for(source_name)
        return matches[-1] if matches else None

    def all_events(self) -> List[AuditEvent]:
        """Return all recorded events, oldest first."""
        return list(self._events)

    def clear(self) -> None:
        """Remove all stored events."""
        self._events.clear()
