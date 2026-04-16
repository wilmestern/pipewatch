"""Concurrency limiter for pipeline metric collection slots."""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class SlotUsage:
    source_name: str
    acquired_at: datetime
    released_at: Optional[datetime] = None

    @property
    def active(self) -> bool:
        return self.released_at is None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.released_at is None:
            return None
        return (self.released_at - self.acquired_at).total_seconds()


class LimiterCapacityError(Exception):
    """Raised when no slots are available."""


class Limiter:
    """Limits concurrent pipeline collections to a fixed number of slots."""

    def __init__(self, max_slots: int = 5) -> None:
        if max_slots < 1:
            raise ValueError("max_slots must be at least 1")
        self._max_slots = max_slots
        self._lock = threading.Lock()
        self._active: Dict[str, SlotUsage] = {}
        self._history: list[SlotUsage] = []

    @property
    def available_slots(self) -> int:
        with self._lock:
            return self._max_slots - len(self._active)

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    def acquire(self, source_name: str) -> SlotUsage:
        with self._lock:
            if len(self._active) >= self._max_slots:
                raise LimiterCapacityError(
                    f"No slots available (max={self._max_slots})"
                )
            usage = SlotUsage(
                source_name=source_name,
                acquired_at=datetime.now(timezone.utc),
            )
            self._active[source_name] = usage
            return usage

    def release(self, source_name: str) -> Optional[SlotUsage]:
        with self._lock:
            usage = self._active.pop(source_name, None)
            if usage is not None:
                usage.released_at = datetime.now(timezone.utc)
                self._history.append(usage)
            return usage

    def is_active(self, source_name: str) -> bool:
        with self._lock:
            return source_name in self._active

    def history(self) -> list[SlotUsage]:
        with self._lock:
            return list(self._history)
