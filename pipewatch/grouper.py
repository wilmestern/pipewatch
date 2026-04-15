"""Group metric results by a shared key for batch processing or reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class MetricGroup:
    """A named collection of metric results sharing a common grouping key."""

    key: str
    results: List[MetricResult] = field(default_factory=list)

    @property
    def healthy_count(self) -> int:
        return sum(1 for r in self.results if r.is_healthy)

    @property
    def unhealthy_count(self) -> int:
        return sum(1 for r in self.results if not r.is_healthy)

    @property
    def is_healthy(self) -> bool:
        return all(r.is_healthy for r in self.results)

    def summary(self) -> str:
        total = len(self.results)
        return (
            f"[{self.key}] {total} result(s): "
            f"{self.healthy_count} healthy, {self.unhealthy_count} unhealthy"
        )


class Grouper:
    """Groups MetricResult objects by a user-supplied key function."""

    def __init__(self, key_fn: Optional[Callable[[MetricResult], str]] = None) -> None:
        # Default: group by source name
        self._key_fn: Callable[[MetricResult], str] = key_fn or (
            lambda r: r.source_name
        )

    def group(self, results: List[MetricResult]) -> Dict[str, MetricGroup]:
        """Partition *results* into MetricGroup objects keyed by key_fn."""
        groups: Dict[str, MetricGroup] = {}
        for result in results:
            key = self._key_fn(result)
            if key not in groups:
                groups[key] = MetricGroup(key=key)
            groups[key].results.append(result)
        return groups

    def unhealthy_groups(self, results: List[MetricResult]) -> List[MetricGroup]:
        """Return only groups that contain at least one unhealthy result."""
        return [
            g for g in self.group(results).values() if not g.is_healthy
        ]
