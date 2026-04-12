"""Checkpoint module for tracking last-seen metric timestamps per source."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class CheckpointEntry:
    source_name: str
    last_seen: datetime
    run_count: int = 0

    def age_seconds(self, now: Optional[datetime] = None) -> float:
        """Return seconds since this checkpoint was last updated."""
        if now is None:
            now = datetime.now(timezone.utc)
        return (now - self.last_seen).total_seconds()


class CheckpointStore:
    """Tracks the last successful collection time for each source."""

    def __init__(self) -> None:
        self._entries: Dict[str, CheckpointEntry] = {}

    def update(self, source_name: str, timestamp: Optional[datetime] = None) -> CheckpointEntry:
        """Record a checkpoint for the given source, defaulting to now."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        existing = self._entries.get(source_name)
        run_count = (existing.run_count + 1) if existing else 1
        entry = CheckpointEntry(
            source_name=source_name,
            last_seen=timestamp,
            run_count=run_count,
        )
        self._entries[source_name] = entry
        return entry

    def get(self, source_name: str) -> Optional[CheckpointEntry]:
        """Return the checkpoint entry for a source, or None if not found."""
        return self._entries.get(source_name)

    def is_stale(self, source_name: str, max_age_seconds: float, now: Optional[datetime] = None) -> bool:
        """Return True if the source has no checkpoint or its age exceeds max_age_seconds."""
        entry = self.get(source_name)
        if entry is None:
            return True
        return entry.age_seconds(now) > max_age_seconds

    def all_sources(self) -> list:
        """Return a list of all tracked source names."""
        return list(self._entries.keys())

    def clear(self, source_name: str) -> None:
        """Remove the checkpoint entry for a source."""
        self._entries.pop(source_name, None)
