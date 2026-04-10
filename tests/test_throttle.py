"""Tests for pipewatch.throttle."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pipewatch.throttle import Throttler, ThrottleRule


def _make_alert(source_name: str, name: str):
    alert = MagicMock()
    alert.source_name = source_name
    alert.name = name
    return alert


@pytest.fixture
def rule():
    return ThrottleRule(source_name="db", alert_name="row_count", min_interval_seconds=60)


@pytest.fixture
def throttler(rule):
    return Throttler(rules=[rule])


def test_not_throttled_before_first_send(throttler):
    assert throttler.is_throttled("db", "row_count") is False


def test_throttled_immediately_after_send(throttler):
    now = datetime(2024, 1, 1, 12, 0, 0)
    throttler.record_sent("db", "row_count", now=now)
    assert throttler.is_throttled("db", "row_count", now=now) is True


def test_not_throttled_after_interval_passes(throttler):
    sent_at = datetime(2024, 1, 1, 12, 0, 0)
    throttler.record_sent("db", "row_count", now=sent_at)
    later = sent_at + timedelta(seconds=61)
    assert throttler.is_throttled("db", "row_count", now=later) is False


def test_still_throttled_before_interval_passes(throttler):
    sent_at = datetime(2024, 1, 1, 12, 0, 0)
    throttler.record_sent("db", "row_count", now=sent_at)
    almost = sent_at + timedelta(seconds=59)
    assert throttler.is_throttled("db", "row_count", now=almost) is True


def test_no_rule_means_never_throttled(throttler):
    assert throttler.is_throttled("other_source", "some_alert") is False


def test_filter_alerts_removes_throttled(throttler):
    now = datetime(2024, 1, 1, 12, 0, 0)
    throttler.record_sent("db", "row_count", now=now)
    alerts = [
        _make_alert("db", "row_count"),
        _make_alert("db", "latency"),
    ]
    result = throttler.filter_alerts(alerts, now=now)
    assert len(result) == 1
    assert result[0].name == "latency"


def test_filter_alerts_returns_all_when_none_throttled(throttler):
    alerts = [
        _make_alert("db", "row_count"),
        _make_alert("db", "latency"),
    ]
    result = throttler.filter_alerts(alerts)
    assert len(result) == 2


def test_filter_alerts_empty_list(throttler):
    assert throttler.filter_alerts([]) == []


def test_throttle_rule_key():
    rule = ThrottleRule(source_name="s3", alert_name="missing_files")
    assert rule.key() == ("s3", "missing_files")


def test_multiple_rules_independent(rule):
    rule2 = ThrottleRule(source_name="api", alert_name="timeout", min_interval_seconds=120)
    throttler = Throttler(rules=[rule, rule2])
    now = datetime(2024, 1, 1, 12, 0, 0)
    throttler.record_sent("db", "row_count", now=now)
    assert throttler.is_throttled("db", "row_count", now=now) is True
    assert throttler.is_throttled("api", "timeout", now=now) is False
