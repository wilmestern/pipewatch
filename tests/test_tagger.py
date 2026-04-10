"""Tests for pipewatch.tagger."""

import pytest
from unittest.mock import MagicMock

from pipewatch.metrics import MetricResult
from pipewatch.tagger import TagRule, TaggedResult, Tagger


def _make_result(source: str, healthy: bool) -> MetricResult:
    m = MagicMock(spec=MetricResult)
    m.source_name = source
    m.is_healthy = healthy
    return m


@pytest.fixture
def healthy_result():
    return _make_result("db", True)


@pytest.fixture
def unhealthy_result():
    return _make_result("api", False)


@pytest.fixture
def tagger():
    return Tagger()


# --- TagRule.matches ---

def test_rule_matches_any_source_when_source_name_is_none(healthy_result):
    rule = TagRule(tag="all")
    assert rule.matches(healthy_result) is True


def test_rule_matches_specific_source(healthy_result):
    rule = TagRule(tag="db-tag", source_name="db")
    assert rule.matches(healthy_result) is True


def test_rule_does_not_match_wrong_source(unhealthy_result):
    rule = TagRule(tag="db-tag", source_name="db")
    assert rule.matches(unhealthy_result) is False


def test_rule_only_unhealthy_skips_healthy(healthy_result):
    rule = TagRule(tag="bad", only_unhealthy=True)
    assert rule.matches(healthy_result) is False


def test_rule_only_unhealthy_matches_unhealthy(unhealthy_result):
    rule = TagRule(tag="bad", only_unhealthy=True)
    assert rule.matches(unhealthy_result) is True


# --- Tagger.tag ---

def test_tag_returns_tagged_result(tagger, healthy_result):
    tagged = tagger.tag(healthy_result)
    assert isinstance(tagged, TaggedResult)
    assert tagged.result is healthy_result


def test_tag_empty_rules_yields_no_tags(tagger, healthy_result):
    tagged = tagger.tag(healthy_result)
    assert tagged.tags == []


def test_tag_applies_matching_rule(unhealthy_result):
    t = Tagger(rules=[TagRule(tag="degraded", only_unhealthy=True)])
    tagged = t.tag(unhealthy_result)
    assert "degraded" in tagged.tags


def test_tag_deduplicates_tags(healthy_result):
    t = Tagger(rules=[
        TagRule(tag="all"),
        TagRule(tag="all"),
    ])
    tagged = t.tag(healthy_result)
    assert tagged.tags.count("all") == 1


def test_tag_multiple_rules(unhealthy_result):
    t = Tagger(rules=[
        TagRule(tag="all"),
        TagRule(tag="bad", only_unhealthy=True),
    ])
    tagged = t.tag(unhealthy_result)
    assert "all" in tagged.tags
    assert "bad" in tagged.tags


# --- Tagger.tag_all ---

def test_tag_all_returns_one_per_result(tagger, healthy_result, unhealthy_result):
    results = [healthy_result, unhealthy_result]
    tagged = tagger.tag_all(results)
    assert len(tagged) == 2


def test_has_tag_true(unhealthy_result):
    t = Tagger(rules=[TagRule(tag="critical", only_unhealthy=True)])
    tagged = t.tag(unhealthy_result)
    assert tagged.has_tag("critical") is True


def test_has_tag_false(healthy_result):
    t = Tagger(rules=[TagRule(tag="critical", only_unhealthy=True)])
    tagged = t.tag(healthy_result)
    assert tagged.has_tag("critical") is False
