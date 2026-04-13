"""Tests for pipewatch/labeler.py"""
import pytest
from datetime import datetime

from pipewatch.alerts import Alert
from pipewatch.labeler import LabelRule, LabeledAlert, Labeler, Severity


def _alert(source: str = "db", name: str = "row_count") -> Alert:
    return Alert(
        source_name=source,
        alert_name=name,
        message="threshold exceeded",
        triggered_at=datetime(2024, 1, 1, 12, 0, 0),
    )


@pytest.fixture
def labeler() -> Labeler:
    lb = Labeler(default_severity=Severity.LOW)
    lb.add_rule(LabelRule(severity=Severity.CRITICAL, source_name="db", alert_name="row_count"))
    lb.add_rule(LabelRule(severity=Severity.HIGH, source_name="api"))
    lb.add_rule(LabelRule(severity=Severity.INFO, alert_name="heartbeat"))
    return lb


def test_label_rule_matches_exact_source_and_name():
    rule = LabelRule(severity=Severity.CRITICAL, source_name="db", alert_name="row_count")
    assert rule.matches(_alert("db", "row_count"))


def test_label_rule_does_not_match_wrong_source():
    rule = LabelRule(severity=Severity.CRITICAL, source_name="db", alert_name="row_count")
    assert not rule.matches(_alert("api", "row_count"))


def test_label_rule_matches_any_source_when_source_name_is_none():
    rule = LabelRule(severity=Severity.INFO, alert_name="heartbeat")
    assert rule.matches(_alert("db", "heartbeat"))
    assert rule.matches(_alert("api", "heartbeat"))


def test_label_rule_matches_any_alert_when_alert_name_is_none():
    rule = LabelRule(severity=Severity.HIGH, source_name="api")
    assert rule.matches(_alert("api", "latency"))
    assert rule.matches(_alert("api", "row_count"))


def test_label_returns_first_matching_rule(labeler):
    result = labeler.label(_alert("db", "row_count"))
    assert result.severity == Severity.CRITICAL


def test_label_falls_back_to_default_when_no_rule_matches(labeler):
    result = labeler.label(_alert("unknown", "unknown"))
    assert result.severity == Severity.LOW


def test_label_all_returns_one_per_alert(labeler):
    alerts = [_alert("db", "row_count"), _alert("api", "latency"), _alert("x", "y")]
    labeled = labeler.label_all(alerts)
    assert len(labeled) == 3


def test_labeled_alert_summary_contains_severity_and_names(labeler):
    la = labeler.label(_alert("db", "row_count"))
    assert "CRITICAL" in la.summary
    assert "db" in la.summary
    assert "row_count" in la.summary


def test_filter_by_severity_excludes_below_minimum(labeler):
    alerts = [
        _alert("db", "row_count"),   # CRITICAL
        _alert("api", "latency"),    # HIGH
        _alert("x", "heartbeat"),   # INFO
        _alert("z", "other"),       # LOW (default)
    ]
    labeled = labeler.label_all(alerts)
    filtered = labeler.filter_by_severity(labeled, Severity.HIGH)
    severities = {la.severity for la in filtered}
    assert Severity.CRITICAL in severities
    assert Severity.HIGH in severities
    assert Severity.INFO not in severities
    assert Severity.LOW not in severities


def test_filter_by_severity_info_returns_all(labeler):
    alerts = [_alert("db", "row_count"), _alert("x", "heartbeat"), _alert("z", "other")]
    labeled = labeler.label_all(alerts)
    filtered = labeler.filter_by_severity(labeled, Severity.INFO)
    assert len(filtered) == len(labeled)
