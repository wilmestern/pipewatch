"""Tests for pipewatch.silencer."""

import time

import pytest

from pipewatch.alerts import Alert
from pipewatch.silencer import SilenceRule, Silencer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FUTURE = time.time() + 3600   # 1 hour from now
PAST   = time.time() - 1      # already expired


def _alert(source: str = "db", name: str = "high_latency") -> Alert:
    return Alert(source_name=source, alert_name=name, message="test", value=99.0, threshold=50.0)


# ---------------------------------------------------------------------------
# SilenceRule tests
# ---------------------------------------------------------------------------

def test_rule_not_expired_when_in_future():
    rule = SilenceRule(source_name="db", alert_name=None, expires_at=FUTURE)
    assert not rule.is_expired()


def test_rule_expired_when_in_past():
    rule = SilenceRule(source_name="db", alert_name=None, expires_at=PAST)
    assert rule.is_expired()


def test_rule_matches_exact_source_and_name():
    rule = SilenceRule(source_name="db", alert_name="high_latency", expires_at=FUTURE)
    assert rule.matches(_alert("db", "high_latency"))


def test_rule_does_not_match_wrong_source():
    rule = SilenceRule(source_name="cache", alert_name="high_latency", expires_at=FUTURE)
    assert not rule.matches(_alert("db", "high_latency"))


def test_rule_wildcard_source_matches_any():
    rule = SilenceRule(source_name=None, alert_name="high_latency", expires_at=FUTURE)
    assert rule.matches(_alert("db", "high_latency"))
    assert rule.matches(_alert("cache", "high_latency"))


def test_rule_wildcard_name_matches_any():
    rule = SilenceRule(source_name="db", alert_name=None, expires_at=FUTURE)
    assert rule.matches(_alert("db", "high_latency"))
    assert rule.matches(_alert("db", "error_rate"))


# ---------------------------------------------------------------------------
# Silencer tests
# ---------------------------------------------------------------------------

@pytest.fixture
def silencer() -> Silencer:
    return Silencer()


def test_empty_silencer_does_not_silence(silencer):
    assert not silencer.is_silenced(_alert())


def test_active_rule_silences_matching_alert(silencer):
    silencer.add_rule(SilenceRule(source_name="db", alert_name="high_latency", expires_at=FUTURE))
    assert silencer.is_silenced(_alert("db", "high_latency"))


def test_expired_rule_does_not_silence(silencer):
    silencer.add_rule(SilenceRule(source_name="db", alert_name="high_latency", expires_at=PAST))
    assert not silencer.is_silenced(_alert("db", "high_latency"))


def test_filter_alerts_removes_silenced(silencer):
    silencer.add_rule(SilenceRule(source_name="db", alert_name=None, expires_at=FUTURE))
    alerts = [_alert("db", "high_latency"), _alert("cache", "error_rate")]
    result = silencer.filter_alerts(alerts)
    assert len(result) == 1
    assert result[0].source_name == "cache"


def test_filter_alerts_returns_all_when_no_rules(silencer):
    alerts = [_alert("db"), _alert("cache")]
    assert silencer.filter_alerts(alerts) == alerts


def test_remove_expired_prunes_rules(silencer):
    silencer.add_rule(SilenceRule(source_name="db", alert_name=None, expires_at=PAST))
    silencer.add_rule(SilenceRule(source_name="cache", alert_name=None, expires_at=FUTURE))
    removed = silencer.remove_expired()
    assert removed == 1
    assert len(silencer.active_rules) == 1


def test_active_rules_excludes_expired(silencer):
    silencer.add_rule(SilenceRule(source_name="db", alert_name=None, expires_at=PAST))
    silencer.add_rule(SilenceRule(source_name="cache", alert_name=None, expires_at=FUTURE))
    assert len(silencer.active_rules) == 1
    assert silencer.active_rules[0].source_name == "cache"
