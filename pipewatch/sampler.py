"""Sampler: controls whether a metric collection run should be sampled/executed
based on a configured sampling rate (0.0–1.0)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SamplerRule:
    source_name: str | None  # None means apply to all sources
    rate: float  # 0.0 = never sample, 1.0 = always sample

    def __post_init__(self) -> None:
        if not (0.0 <= self.rate <= 1.0):
            raise ValueError(f"Sampling rate must be between 0.0 and 1.0, got {self.rate}")

    def matches(self, source_name: str) -> bool:
        return self.source_name is None or self.source_name == source_name


@dataclass
class SampleDecision:
    source_name: str
    sampled: bool
    rate_applied: float

    def __bool__(self) -> bool:
        return self.sampled


class Sampler:
    """Determines whether a given source should be collected on this cycle."""

    def __init__(self, rules: list[SamplerRule] | None = None, default_rate: float = 1.0) -> None:
        if not (0.0 <= default_rate <= 1.0):
            raise ValueError(f"default_rate must be between 0.0 and 1.0, got {default_rate}")
        self._rules: list[SamplerRule] = rules or []
        self._default_rate = default_rate
        self._counts: Dict[str, int] = {}
        self._sampled_counts: Dict[str, int] = {}

    def _rate_for(self, source_name: str) -> float:
        for rule in self._rules:
            if rule.matches(source_name):
                return rule.rate
        return self._default_rate

    def should_collect(self, source_name: str, rng: random.Random | None = None) -> SampleDecision:
        """Return a SampleDecision indicating whether this source should run."""
        rate = self._rate_for(source_name)
        roller = rng or random
        sampled = roller.random() < rate
        self._counts[source_name] = self._counts.get(source_name, 0) + 1
        if sampled:
            self._sampled_counts[source_name] = self._sampled_counts.get(source_name, 0) + 1
        return SampleDecision(source_name=source_name, sampled=sampled, rate_applied=rate)

    def stats(self, source_name: str) -> dict:
        total = self._counts.get(source_name, 0)
        sampled = self._sampled_counts.get(source_name, 0)
        return {
            "source": source_name,
            "total_checks": total,
            "sampled": sampled,
            "skipped": total - sampled,
        }
