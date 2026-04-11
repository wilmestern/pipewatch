"""Alert deduplication — suppress repeated alerts for the same condition."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

from pipewatch.alerts import Alert


@dataclass
class DeduplicationRecord:
    """Tracks the last time a specific alert was forwarded."""

    source_name: str
    alert_name: str
    first_seen: datetime
    last_forwarded: datetime
    suppressed_count: int = 0

    @property
    def key(self) -> Tuple[str, str]:
        return (self.source_name, self.alert_name)


class Deduplicator:
    """Suppresses duplicate alerts that fire repeatedly within a cooldown window.

    An alert is considered a duplicate if it has already been forwarded within
    ``cooldown_seconds`` seconds.  After the cooldown expires the alert is
    forwarded again and the timer resets.
    """

    def __init__(self, cooldown_seconds: int = 300) -> None:
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")
        self.cooldown_seconds = cooldown_seconds
        self._records: Dict[Tuple[str, str], DeduplicationRecord] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_duplicate(self, alert: Alert, now: Optional[datetime] = None) -> bool:
        """Return True if *alert* should be suppressed as a duplicate."""
        now = now or datetime.now(timezone.utc)
        key = (alert.source_name, alert.alert_name)
        record = self._records.get(key)

        if record is None:
            # First time we see this alert — record and allow through.
            self._records[key] = DeduplicationRecord(
                source_name=alert.source_name,
                alert_name=alert.alert_name,
                first_seen=now,
                last_forwarded=now,
            )
            return False

        elapsed = (now - record.last_forwarded).total_seconds()
        if elapsed < self.cooldown_seconds:
            record.suppressed_count += 1
            return True

        # Cooldown expired — allow through and reset the timer.
        record.last_forwarded = now
        record.suppressed_count = 0
        return False

    def reset(self, source_name: str, alert_name: str) -> None:
        """Remove the deduplication record for a specific alert."""
        self._records.pop((source_name, alert_name), None)

    def record_for(self, source_name: str, alert_name: str) -> Optional[DeduplicationRecord]:
        """Return the current record for an alert, or None if unseen."""
        return self._records.get((source_name, alert_name))

    def suppressed_count(self, source_name: str, alert_name: str) -> int:
        """Return how many times this alert has been suppressed since last forward."""
        record = self._records.get((source_name, alert_name))
        return record.suppressed_count if record else 0
