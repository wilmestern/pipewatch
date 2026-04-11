"""Tests for pipewatch.deduplicator."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.alerts import Alert
from pipewatch.deduplicator import Deduplicator, DeduplicationRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _alert(source: str = "db", name: str = "row_count") -> Alert:
    return Alert(source_name=source, alert_name=name, message="threshold breached", value=0.0, threshold=1.0)


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def deduplicator() -> Deduplicator:
    return Deduplicator(cooldown_seconds=300)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_first_alert_is_not_duplicate(deduplicator):
    assert deduplicator.is_duplicate(_alert(), now=NOW) is False


def test_immediate_repeat_is_duplicate(deduplicator):
    deduplicator.is_duplicate(_alert(), now=NOW)
    assert deduplicator.is_duplicate(_alert(), now=NOW) is True


def test_repeat_after_cooldown_is_not_duplicate(deduplicator):
    deduplicator.is_duplicate(_alert(), now=NOW)
    later = NOW + timedelta(seconds=301)
    assert deduplicator.is_duplicate(_alert(), now=later) is False


def test_repeat_exactly_at_cooldown_boundary_is_duplicate(deduplicator):
    deduplicator.is_duplicate(_alert(), now=NOW)
    boundary = NOW + timedelta(seconds=300)
    # elapsed == cooldown_seconds → still within window
    assert deduplicator.is_duplicate(_alert(), now=boundary) is True


def test_suppressed_count_increments(deduplicator):
    a = _alert()
    deduplicator.is_duplicate(a, now=NOW)
    deduplicator.is_duplicate(a, now=NOW)
    deduplicator.is_duplicate(a, now=NOW)
    assert deduplicator.suppressed_count("db", "row_count") == 2


def test_suppressed_count_resets_after_cooldown(deduplicator):
    a = _alert()
    deduplicator.is_duplicate(a, now=NOW)
    deduplicator.is_duplicate(a, now=NOW)  # suppressed
    later = NOW + timedelta(seconds=400)
    deduplicator.is_duplicate(a, now=later)  # forwarded again
    assert deduplicator.suppressed_count("db", "row_count") == 0


def test_different_alerts_tracked_independently(deduplicator):
    a1 = _alert(source="db", name="row_count")
    a2 = _alert(source="api", name="latency")
    deduplicator.is_duplicate(a1, now=NOW)
    # a2 has never been seen — should not be duplicate
    assert deduplicator.is_duplicate(a2, now=NOW) is False


def test_reset_clears_record(deduplicator):
    a = _alert()
    deduplicator.is_duplicate(a, now=NOW)
    deduplicator.reset("db", "row_count")
    # After reset the next call should behave as first-time
    assert deduplicator.is_duplicate(a, now=NOW) is False


def test_record_for_returns_none_when_unseen(deduplicator):
    assert deduplicator.record_for("db", "row_count") is None


def test_record_for_returns_record_after_first_alert(deduplicator):
    deduplicator.is_duplicate(_alert(), now=NOW)
    record = deduplicator.record_for("db", "row_count")
    assert isinstance(record, DeduplicationRecord)
    assert record.first_seen == NOW


def test_negative_cooldown_raises():
    with pytest.raises(ValueError):
        Deduplicator(cooldown_seconds=-1)


def test_zero_cooldown_never_suppresses(deduplicator):
    d = Deduplicator(cooldown_seconds=0)
    a = _alert()
    d.is_duplicate(a, now=NOW)
    # elapsed == 0 which is NOT < 0, so should forward again
    assert d.is_duplicate(a, now=NOW) is False
