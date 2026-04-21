"""Classifier: categorize metric results into named buckets based on rules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class ClassifyRule:
    """Maps a condition to a category label."""
    category: str
    source_name: Optional[str] = None   # None = match any source
    only_unhealthy: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def matches(self, result: MetricResult) -> bool:
        if self.source_name and result.source.name != self.source_name:
            return False
        if self.only_unhealthy and result.is_healthy:
            return False
        value = result.metric.value if result.metric else None
        if self.min_value is not None and (value is None or value < self.min_value):
            return False
        if self.max_value is not None and (value is None or value > self.max_value):
            return False
        return True


@dataclass
class ClassifiedResult:
    """A metric result decorated with its assigned category."""
    result: MetricResult
    category: str

    def summary(self) -> str:
        status = "healthy" if self.result.is_healthy else "unhealthy"
        return f"[{self.category}] {self.result.source.name} — {status}"


class Classifier:
    """Applies an ordered list of ClassifyRules to MetricResults."""

    def __init__(self, rules: List[ClassifyRule], default_category: str = "uncategorized") -> None:
        if not default_category:
            raise ValueError("default_category must be a non-empty string")
        self._rules = rules
        self._default = default_category

    def classify(self, result: MetricResult) -> ClassifiedResult:
        """Return the first matching category, or the default."""
        for rule in self._rules:
            if rule.matches(result):
                return ClassifiedResult(result=result, category=rule.category)
        return ClassifiedResult(result=result, category=self._default)

    def classify_all(self, results: List[MetricResult]) -> List[ClassifiedResult]:
        """Classify every result in the list."""
        return [self.classify(r) for r in results]

    def by_category(self, results: List[MetricResult]) -> dict[str, List[ClassifiedResult]]:
        """Group classified results by category label."""
        grouped: dict[str, List[ClassifiedResult]] = {}
        for cr in self.classify_all(results):
            grouped.setdefault(cr.category, []).append(cr)
        return grouped
