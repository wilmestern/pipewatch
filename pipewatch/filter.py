"""Filter module for pipewatch — supports filtering pipeline metric results by various criteria."""

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.metrics import MetricResult


@dataclass
class FilterCriteria:
    """Criteria used to filter MetricResult entries."""
    source_name: Optional[str] = None
    only_unhealthy: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    error_contains: Optional[str] = None

    def matches(self, result: MetricResult) -> bool:
        """Return True if the given MetricResult satisfies all criteria."""
        if self.source_name is not None:
            if result.source_name != self.source_name:
                return False

        if self.only_unhealthy and result.is_healthy:
            return False

        if self.min_value is not None and result.value is not None:
            if result.value < self.min_value:
                return False

        if self.max_value is not None and result.value is not None:
            if result.value > self.max_value:
                return False

        if self.error_contains is not None:
            if result.error is None or self.error_contains not in result.error:
                return False

        return True


class MetricFilter:
    """Applies FilterCriteria to a collection of MetricResults."""

    def __init__(self, criteria: FilterCriteria) -> None:
        self.criteria = criteria

    def apply(self, results: List[MetricResult]) -> List[MetricResult]:
        """Return only those results that match the filter criteria."""
        return [r for r in results if self.criteria.matches(r)]

    def first(self, results: List[MetricResult]) -> Optional[MetricResult]:
        """Return the first matching result, or None."""
        for r in results:
            if self.criteria.matches(r):
                return r
        return None
