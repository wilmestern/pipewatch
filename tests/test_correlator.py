"""Tests for pipewatch.correlator."""
from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert
from pipewatch.correlator import CorrelationRule, Correlator


T0 = datetime(2024, 1, 1, 12, 0, 0)


def _alert(source: str, name: str) -> Alert:
    return Alert(source_name=source, alert_name=name, message=f"{source}/{name}")


@pytest.fixture
def rule() -> CorrelationRule:
    return CorrelationRule(
        name="db_and_api_lag",
        source_names=["db", "api"],
        alert_names=["high_latency"],
        window_seconds=60,
    )


@pytest.fixture
def correlator(rule: CorrelationRule) -> Correlator:
    return Correlator(rules=[rule])


def test_no_match_for_single_source(correlator: Correlator) -> None:
    matches = correlator.evaluate(_alert("db", "high_latency"), now=T0)
    assert matches == []


def test_match_when_both_sources_alert_within_window(correlator: Correlator) -> None:
    correlator.evaluate(_alert("db", "high_latency"), now=T0)
    matches = correlator.evaluate(_alert("api", "high_latency"), now=T0 + timedelta(seconds=30))
    assert len(matches) == 1
    assert matches[0].rule_name == "db_and_api_lag"


def test_no_match_when_second_alert_outside_window(correlator: Correlator) -> None:
    correlator.evaluate(_alert("db", "high_latency"), now=T0)
    matches = correlator.evaluate(_alert("api", "high_latency"), now=T0 + timedelta(seconds=90))
    assert matches == []


def test_match_contains_both_alerts(correlator: Correlator) -> None:
    correlator.evaluate(_alert("db", "high_latency"), now=T0)
    matches = correlator.evaluate(_alert("api", "high_latency"), now=T0 + timedelta(seconds=10))
    assert len(matches[0].matched_alerts) == 2
    sources = {a.source_name for a in matches[0].matched_alerts}
    assert sources == {"db", "api"}


def test_buffer_reset_after_match(correlator: Correlator) -> None:
    correlator.evaluate(_alert("db", "high_latency"), now=T0)
    correlator.evaluate(_alert("api", "high_latency"), now=T0 + timedelta(seconds=5))
    # Second pair — db fires again but api hasn't yet
    matches = correlator.evaluate(_alert("db", "high_latency"), now=T0 + timedelta(seconds=10))
    assert matches == []


def test_irrelevant_alert_ignored(correlator: Correlator) -> None:
    matches = correlator.evaluate(_alert("cache", "high_latency"), now=T0)
    assert matches == []


def test_wrong_alert_name_not_matched(correlator: Correlator) -> None:
    correlator.evaluate(_alert("db", "connection_error"), now=T0)
    matches = correlator.evaluate(_alert("api", "connection_error"), now=T0 + timedelta(seconds=5))
    assert matches == []


def test_correlation_match_summary(correlator: Correlator) -> None:
    correlator.evaluate(_alert("db", "high_latency"), now=T0)
    matches = correlator.evaluate(_alert("api", "high_latency"), now=T0 + timedelta(seconds=1))
    assert "db_and_api_lag" in matches[0].summary


def test_add_rule_dynamically() -> None:
    c = Correlator()
    r = CorrelationRule("test", ["src_a", "src_b"], ["err"], window_seconds=30)
    c.add_rule(r)
    c.evaluate(_alert("src_a", "err"), now=T0)
    matches = c.evaluate(_alert("src_b", "err"), now=T0 + timedelta(seconds=5))
    assert len(matches) == 1
