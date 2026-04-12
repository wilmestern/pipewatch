"""Tests for pipewatch.sampler."""

import random
import pytest

from pipewatch.sampler import Sampler, SamplerRule, SampleDecision


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def always_rule() -> SamplerRule:
    return SamplerRule(source_name="always_src", rate=1.0)


@pytest.fixture
def never_rule() -> SamplerRule:
    return SamplerRule(source_name="never_src", rate=0.0)


@pytest.fixture
def sampler(always_rule, never_rule) -> Sampler:
    return Sampler(rules=[always_rule, never_rule], default_rate=1.0)


# ---------------------------------------------------------------------------
# SamplerRule tests
# ---------------------------------------------------------------------------

def test_rule_rejects_rate_above_one():
    with pytest.raises(ValueError):
        SamplerRule(source_name="s", rate=1.5)


def test_rule_rejects_negative_rate():
    with pytest.raises(ValueError):
        SamplerRule(source_name="s", rate=-0.1)


def test_rule_matches_exact_source():
    rule = SamplerRule(source_name="db", rate=0.5)
    assert rule.matches("db") is True
    assert rule.matches("other") is False


def test_rule_with_none_source_matches_any():
    rule = SamplerRule(source_name=None, rate=0.5)
    assert rule.matches("anything") is True


# ---------------------------------------------------------------------------
# Sampler.should_collect tests
# ---------------------------------------------------------------------------

def test_always_sampled_returns_true(sampler):
    decision = sampler.should_collect("always_src")
    assert decision.sampled is True
    assert bool(decision) is True


def test_never_sampled_returns_false(sampler):
    decision = sampler.should_collect("never_src")
    assert decision.sampled is False
    assert bool(decision) is False


def test_decision_contains_correct_source(sampler):
    decision = sampler.should_collect("always_src")
    assert decision.source_name == "always_src"


def test_decision_contains_rate_applied(sampler):
    decision = sampler.should_collect("never_src")
    assert decision.rate_applied == 0.0


def test_default_rate_used_for_unknown_source():
    s = Sampler(default_rate=1.0)
    decision = s.should_collect("unknown")
    assert decision.sampled is True
    assert decision.rate_applied == 1.0


def test_default_rate_zero_never_samples():
    s = Sampler(default_rate=0.0)
    for _ in range(10):
        assert s.should_collect("src").sampled is False


def test_sampler_rejects_invalid_default_rate():
    with pytest.raises(ValueError):
        Sampler(default_rate=2.0)


def test_custom_rng_controls_outcome():
    s = Sampler(default_rate=0.5)
    rng_high = random.Random(0)
    rng_high.random = lambda: 0.9  # type: ignore[method-assign]
    assert s.should_collect("src", rng=rng_high).sampled is False

    rng_low = random.Random(0)
    rng_low.random = lambda: 0.1  # type: ignore[method-assign]
    assert s.should_collect("src", rng=rng_low).sampled is True


# ---------------------------------------------------------------------------
# Sampler.stats tests
# ---------------------------------------------------------------------------

def test_stats_returns_zero_when_no_checks():
    s = Sampler()
    stats = s.stats("new_src")
    assert stats["total_checks"] == 0
    assert stats["sampled"] == 0
    assert stats["skipped"] == 0


def test_stats_counts_correctly(sampler):
    for _ in range(5):
        sampler.should_collect("always_src")
    stats = sampler.stats("always_src")
    assert stats["total_checks"] == 5
    assert stats["sampled"] == 5
    assert stats["skipped"] == 0


def test_stats_skipped_when_never_sampled(sampler):
    for _ in range(3):
        sampler.should_collect("never_src")
    stats = sampler.stats("never_src")
    assert stats["total_checks"] == 3
    assert stats["sampled"] == 0
    assert stats["skipped"] == 3
