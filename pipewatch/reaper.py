"""Reaper: removes stale or inactive sources from tracking based on inactivity thresholds."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pipewatch.checkpoint import CheckpointStore


@dataclass
class ReaperPolicy:
    max_inactivity_seconds: int
    dry_run: bool = False

    def __post_init__(self) -> None:
        if self.max_inactivity_seconds <= 0:
            raise ValueError("max_inactivity_seconds must be positive")


@dataclass
class ReapResult:
    source_name: str
    last_seen: datetime
    reaped_at: datetime
    dry_run: bool

    def summary(self) -> str:
        prefix = "[DRY RUN] " if self.dry_run else ""
        age = (self.reaped_at - self.last_seen).seconds
        return f"{prefix}Reaped '{self.source_name}' (inactive for {age}s)"


class Reaper:
    def __init__(self, policy: ReaperPolicy, store: CheckpointStore) -> None:
        self._policy = policy
        self._store = store
        self._reaped: List[ReapResult] = []

    def run(self, now: Optional[datetime] = None) -> List[ReapResult]:
        """Evaluate all tracked sources and reap those exceeding inactivity threshold."""
        now = now or datetime.utcnow()
        cutoff = now - timedelta(seconds=self._policy.max_inactivity_seconds)
        results: List[ReapResult] = []

        for source_name in list(self._store.all_sources()):
            entry = self._store.get(source_name)
            if entry is None:
                continue
            if entry.last_run < cutoff:
                result = ReapResult(
                    source_name=source_name,
                    last_seen=entry.last_run,
                    reaped_at=now,
                    dry_run=self._policy.dry_run,
                )
                results.append(result)
                if not self._policy.dry_run:
                    self._store.remove(source_name)
                self._reaped.append(result)

        return results

    def reaped_sources(self) -> List[ReapResult]:
        return list(self._reaped)
