"""Rate limiting for metric collection to prevent overwhelming data sources."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class RateLimitRule:
    source_name: str
    max_calls: int
    window_seconds: int

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class RateLimitState:
    call_times: list = field(default_factory=list)

    def prune(self, window: timedelta) -> None:
        """Remove call timestamps outside the current window."""
        cutoff = datetime.utcnow() - window
        self.call_times = [t for t in self.call_times if t >= cutoff]

    def count_in_window(self, window: timedelta) -> int:
        self.prune(window)
        return len(self.call_times)

    def record_call(self) -> None:
        self.call_times.append(datetime.utcnow())


class RateLimiter:
    """Tracks and enforces per-source rate limits for metric collection."""

    def __init__(self, rules: Optional[list[RateLimitRule]] = None) -> None:
        self._rules: Dict[str, RateLimitRule] = {}
        self._states: Dict[str, RateLimitState] = {}
        for rule in (rules or []):
            self.add_rule(rule)

    def add_rule(self, rule: RateLimitRule) -> None:
        self._rules[rule.source_name] = rule
        self._states.setdefault(rule.source_name, RateLimitState())

    def is_rate_limited(self, source_name: str) -> bool:
        """Return True if the source has exceeded its allowed call rate."""
        if source_name not in self._rules:
            return False
        rule = self._rules[source_name]
        state = self._states[source_name]
        return state.count_in_window(rule.window) >= rule.max_calls

    def record_call(self, source_name: str) -> None:
        """Record that a collection call was made for the given source."""
        if source_name not in self._states:
            self._states[source_name] = RateLimitState()
        self._states[source_name].record_call()

    def remaining_calls(self, source_name: str) -> Optional[int]:
        """Return how many calls remain in the current window, or None if no rule."""
        if source_name not in self._rules:
            return None
        rule = self._rules[source_name]
        state = self._states[source_name]
        used = state.count_in_window(rule.window)
        return max(0, rule.max_calls - used)
