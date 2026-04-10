"""Tag-based labeling for pipeline metric results."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class TagRule:
    """A rule that assigns a tag to a metric result based on source or health."""

    tag: str
    source_name: Optional[str] = None  # None means match any source
    only_unhealthy: bool = False

    def matches(self, result: MetricResult) -> bool:
        """Return True if this rule applies to the given result."""
        if self.source_name is not None and result.source_name != self.source_name:
            return False
        if self.only_unhealthy and result.is_healthy:
            return False
        return True


@dataclass
class TaggedResult:
    """A metric result decorated with a set of tags."""

    result: MetricResult
    tags: List[str] = field(default_factory=list)

    def has_tag(self, tag: str) -> bool:
        return tag in self.tags


class Tagger:
    """Applies tag rules to metric results and returns tagged results."""

    def __init__(self, rules: Optional[List[TagRule]] = None) -> None:
        self._rules: List[TagRule] = rules or []

    def add_rule(self, rule: TagRule) -> None:
        """Register a new tagging rule."""
        self._rules.append(rule)

    def tag(self, result: MetricResult) -> TaggedResult:
        """Apply all matching rules to a single result."""
        tags = [
            rule.tag for rule in self._rules if rule.matches(result)
        ]
        # Deduplicate while preserving order
        seen: Dict[str, None] = {}
        for t in tags:
            seen[t] = None
        return TaggedResult(result=result, tags=list(seen.keys()))

    def tag_all(self, results: List[MetricResult]) -> List[TaggedResult]:
        """Apply tagging to a list of results."""
        return [self.tag(r) for r in results]
