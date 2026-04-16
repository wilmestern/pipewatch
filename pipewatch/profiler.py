"""Profiler: tracks execution duration of pipeline metric collection runs."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ProfileEntry:
    source_name: str
    duration_seconds: float
    timestamp: float = field(default_factory=time.time)

    @property
    def slow(self) -> bool:
        return self.duration_seconds > self.threshold if hasattr(self, "threshold") else False

    def summary(self) -> str:
        return f"{self.source_name}: {self.duration_seconds:.3f}s"


@dataclass
class ProfileReport:
    entries: List[ProfileEntry]

    @property
    def total_sources(self) -> int:
        return len(self.entries)

    @property
    def average_duration(self) -> float:
        if not self.entries:
            return 0.0
        return sum(e.duration_seconds for e in self.entries) / len(self.entries)

    @property
    def slowest(self) -> Optional[ProfileEntry]:
        return max(self.entries, key=lambda e: e.duration_seconds) if self.entries else None

    def summary(self) -> str:
        if not self.entries:
            return "No profiling data."
        lines = [e.summary() for e in self.entries]
        lines.append(f"avg={self.average_duration:.3f}s")
        if self.slowest:
            lines.append(f"slowest={self.slowest.source_name}")
        return " | ".join(lines)


class Profiler:
    def __init__(self, slow_threshold_seconds: float = 5.0) -> None:
        self.slow_threshold_seconds = slow_threshold_seconds
        self._entries: Dict[str, List[ProfileEntry]] = {}

    def record(self, source_name: str, duration_seconds: float) -> ProfileEntry:
        entry = ProfileEntry(source_name=source_name, duration_seconds=duration_seconds)
        self._entries.setdefault(source_name, []).append(entry)
        return entry

    def is_slow(self, source_name: str) -> bool:
        entries = self._entries.get(source_name, [])
        if not entries:
            return False
        return entries[-1].duration_seconds > self.slow_threshold_seconds

    def latest(self, source_name: str) -> Optional[ProfileEntry]:
        entries = self._entries.get(source_name, [])
        return entries[-1] if entries else None

    def report(self) -> ProfileReport:
        latest_entries = [
            entries[-1] for entries in self._entries.values() if entries
        ]
        return ProfileReport(entries=latest_entries)

    def clear(self) -> None:
        self._entries.clear()
