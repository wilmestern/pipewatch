"""Batcher: groups MetricResults into fixed-size or time-bounded batches."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class Batch:
    source_name: str
    results: List[MetricResult] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def size(self) -> int:
        return len(self.results)

    @property
    def healthy_count(self) -> int:
        return sum(1 for r in self.results if r.is_healthy)

    @property
    def unhealthy_count(self) -> int:
        return self.size - self.healthy_count

    def summary(self) -> str:
        return (
            f"Batch({self.source_name}: {self.size} results, "
            f"{self.healthy_count} healthy, {self.unhealthy_count} unhealthy)"
        )


class Batcher:
    """Accumulates MetricResults and flushes them in batches."""

    def __init__(self, max_size: int = 10) -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self._max_size = max_size
        self._buffers: dict[str, List[MetricResult]] = {}

    def add(self, result: MetricResult) -> Optional[Batch]:
        """Add a result. Returns a flushed Batch if the buffer is full."""
        name = result.source_name
        self._buffers.setdefault(name, [])
        self._buffers[name].append(result)
        if len(self._buffers[name]) >= self._max_size:
            return self._flush(name)
        return None

    def flush(self, source_name: str) -> Optional[Batch]:
        """Manually flush the buffer for a source, even if not full."""
        if source_name not in self._buffers or not self._buffers[source_name]:
            return None
        return self._flush(source_name)

    def flush_all(self) -> List[Batch]:
        """Flush all non-empty buffers."""
        batches = []
        for name in list(self._buffers.keys()):
            batch = self.flush(name)
            if batch is not None:
                batches.append(batch)
        return batches

    def pending_count(self, source_name: str) -> int:
        return len(self._buffers.get(source_name, []))

    def _flush(self, source_name: str) -> Batch:
        results = self._buffers.pop(source_name)
        return Batch(source_name=source_name, results=results)
