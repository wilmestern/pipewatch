"""Tests for pipewatch.auditor."""

from datetime import datetime, timezone

import pytest

from pipewatch.auditor import AuditEvent, Auditor


@pytest.fixture()
def auditor() -> Auditor:
    return Auditor(max_events=100)


def test_record_returns_audit_event(auditor: Auditor) -> None:
    event = auditor.record("db", "alert_fired", "latency exceeded threshold")
    assert isinstance(event, AuditEvent)
    assert event.source_name == "db"
    assert event.event_type == "alert_fired"
    assert event.detail == "latency exceeded threshold"


def test_record_sets_timestamp(auditor: Auditor) -> None:
    before = datetime.now(timezone.utc)
    event = auditor.record("db", "metric_collected", "ok")
    after = datetime.now(timezone.utc)
    assert before <= event.timestamp <= after


def test_events_for_returns_only_matching_source(auditor: Auditor) -> None:
    auditor.record("db", "alert_fired", "high latency")
    auditor.record("queue", "alert_fired", "queue depth")
    auditor.record("db", "alert_resolved", "latency normal")
    result = auditor.events_for("db")
    assert len(result) == 2
    assert all(e.source_name == "db" for e in result)


def test_events_for_returns_empty_for_unknown_source(auditor: Auditor) -> None:
    auditor.record("db", "alert_fired", "x")
    assert auditor.events_for("unknown") == []


def test_events_by_type_filters_correctly(auditor: Auditor) -> None:
    auditor.record("db", "alert_fired", "a")
    auditor.record("db", "alert_resolved", "b")
    auditor.record("queue", "alert_fired", "c")
    fired = auditor.events_by_type("alert_fired")
    assert len(fired) == 2
    assert all(e.event_type == "alert_fired" for e in fired)


def test_latest_returns_most_recent_event(auditor: Auditor) -> None:
    auditor.record("db", "alert_fired", "first")
    auditor.record("db", "alert_resolved", "second")
    latest = auditor.latest("db")
    assert latest is not None
    assert latest.detail == "second"


def test_latest_returns_none_when_empty(auditor: Auditor) -> None:
    assert auditor.latest("db") is None


def test_all_events_returns_all(auditor: Auditor) -> None:
    auditor.record("db", "alert_fired", "a")
    auditor.record("queue", "metric_collected", "b")
    assert len(auditor.all_events()) == 2


def test_clear_removes_all_events(auditor: Auditor) -> None:
    auditor.record("db", "alert_fired", "x")
    auditor.clear()
    assert auditor.all_events() == []


def test_max_events_cap_is_respected() -> None:
    auditor = Auditor(max_events=3)
    for i in range(6):
        auditor.record("src", "metric_collected", f"run {i}")
    events = auditor.all_events()
    assert len(events) == 3
    assert events[-1].detail == "run 5"


def test_max_events_rejects_non_positive() -> None:
    with pytest.raises(ValueError):
        Auditor(max_events=0)


def test_summary_format(auditor: Auditor) -> None:
    event = auditor.record("db", "alert_fired", "latency spike")
    summary = event.summary()
    assert "alert_fired" in summary
    assert "db" in summary
    assert "latency spike" in summary
