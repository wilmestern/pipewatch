"""Tests for pipewatch.classifier."""
import pytest
from unittest.mock import MagicMock

from pipewatch.classifier import ClassifyRule, ClassifiedResult, Classifier
from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def source_config():
    return SourceConfig(name="db", type="postgres", connection="postgresql://localhost/test")


@pytest.fixture()
def _make_result(source_config):
    def _inner(value: float, healthy: bool = True) -> MetricResult:
        metric = PipelineMetric(source_name=source_config.name, value=value)
        return MetricResult(source=source_config, metric=metric, is_healthy=healthy)
    return _inner


@pytest.fixture()
def rules():
    return [
        ClassifyRule(category="critical", only_unhealthy=True, min_value=90.0),
        ClassifyRule(category="degraded", only_unhealthy=True),
        ClassifyRule(category="nominal", only_unhealthy=False),
    ]


@pytest.fixture()
def classifier(rules):
    return Classifier(rules=rules, default_category="unknown")


# ---------------------------------------------------------------------------
# ClassifyRule.matches
# ---------------------------------------------------------------------------

def test_rule_matches_any_source_when_source_name_is_none(_make_result):
    rule = ClassifyRule(category="all")
    assert rule.matches(_make_result(1.0)) is True


def test_rule_does_not_match_wrong_source(source_config, _make_result):
    rule = ClassifyRule(category="x", source_name="other")
    assert rule.matches(_make_result(1.0)) is False


def test_rule_matches_specific_source(_make_result):
    rule = ClassifyRule(category="x", source_name="db")
    assert rule.matches(_make_result(1.0)) is True


def test_rule_only_unhealthy_skips_healthy(_make_result):
    rule = ClassifyRule(category="bad", only_unhealthy=True)
    assert rule.matches(_make_result(1.0, healthy=True)) is False


def test_rule_only_unhealthy_matches_unhealthy(_make_result):
    rule = ClassifyRule(category="bad", only_unhealthy=True)
    assert rule.matches(_make_result(1.0, healthy=False)) is True


def test_rule_min_value_rejects_below(_make_result):
    rule = ClassifyRule(category="high", min_value=50.0)
    assert rule.matches(_make_result(30.0)) is False


def test_rule_max_value_rejects_above(_make_result):
    rule = ClassifyRule(category="low", max_value=10.0)
    assert rule.matches(_make_result(20.0)) is False


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

def test_default_category_required():
    with pytest.raises(ValueError):
        Classifier(rules=[], default_category="")


def test_classify_returns_first_match(_make_result, classifier):
    result = _make_result(95.0, healthy=False)
    cr = classifier.classify(result)
    assert cr.category == "critical"


def test_classify_falls_through_to_degraded(_make_result, classifier):
    result = _make_result(40.0, healthy=False)
    cr = classifier.classify(result)
    assert cr.category == "degraded"


def test_classify_nominal_for_healthy(_make_result, classifier):
    result = _make_result(50.0, healthy=True)
    cr = classifier.classify(result)
    assert cr.category == "nominal"


def test_classify_all_returns_correct_count(_make_result, classifier):
    results = [_make_result(1.0), _make_result(2.0, healthy=False)]
    classified = classifier.classify_all(results)
    assert len(classified) == 2


def test_by_category_groups_correctly(_make_result, classifier):
    results = [
        _make_result(1.0, healthy=True),
        _make_result(2.0, healthy=False),
        _make_result(95.0, healthy=False),
    ]
    grouped = classifier.by_category(results)
    assert "nominal" in grouped
    assert "degraded" in grouped
    assert "critical" in grouped


def test_classified_result_summary(_make_result, classifier):
    cr = classifier.classify(_make_result(1.0, healthy=True))
    assert "nominal" in cr.summary()
    assert "db" in cr.summary()
