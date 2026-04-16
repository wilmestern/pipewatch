"""Quota enforcement for pipeline metric collection runs."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List


@dataclass
class QuotaRule:
    source_name: str | None  # None = applies to all sources
    max_runs: int
    window_seconds: int

    @property
    def window(self) -> timedelta:
        return timedelta(seconds=self.window_seconds)


@dataclass
class QuotaState:
    timestamps: List[datetime] = field(default_factory=list)

    def prune(self, window: timedelta) -> None:
        cutoff = datetime.utcnow() - window
        self.timestamps = [t for t in self.timestamps if t >= cutoff]

    def count_in_window(self, window: timedelta) -> int:
        self.prune(window)
        return len(self.timestamps)

    def record(self) -> None:
        self.timestamps.append(datetime.utcnow())


@dataclass
class QuotaResult:
    source_name: str
    allowed: bool
    current_count: int
    max_runs: int
    window_seconds: int

    def summary(self) -> str:
        status = "allowed" if self.allowed else "denied"
        return (
            f"{self.source_name}: {status} "
            f"({self.current_count}/{self.max_runs} in {self.window_seconds}s)"
        )


class QuotaManager:
    def __init__(self, rules: List[QuotaRule]) -> None:
        self._rules = rules
        self._states: Dict[str, QuotaState] = {}

    def _rule_for(self, source_name: str) -> QuotaRule | None:
        for rule in self._rules:
            if rule.source_name is None or rule.source_name == source_name:
                return rule
        return None

    def check(self, source_name: str) -> QuotaResult:
        rule = self._rule_for(source_name)
        if rule is None:
            return QuotaResult(source_name, True, 0, 0, 0)
        state = self._states.setdefault(source_name, QuotaState())
        count = state.count_in_window(rule.window)
        allowed = count < rule.max_runs
        return QuotaResult(source_name, allowed, count, rule.max_runs, rule.window_seconds)

    def record(self, source_name: str) -> None:
        rule = self._rule_for(source_name)
        if rule is None:
            return
        state = self._states.setdefault(source_name, QuotaState())
        state.record()

    def check_and_record(self, source_name: str) -> QuotaResult:
        result = self.check(source_name)
        if result.allowed:
            self.record(source_name)
        return result
