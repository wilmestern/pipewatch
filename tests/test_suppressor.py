"""Tests for pipewatch.suppressor."""

from datetime import datetime, time

import pytest

from pipewatch.alerts import Alert
from pipewatch.suppressor import SuppressionWindow, Suppressor, SuppressResult


def _alert(source: str = "db", name: str = "row_count") -> Alert:
    return Alert(source_name=source, alert_name=name, message="test alert")


# ---------------------------------------------------------------------------
# SuppressionWindow.matches
# ---------------------------------------------------------------------------

def test_window_matches_any_source_when_source_name_is_none():
    window = SuppressionWindow(source_name=None, alert_name=None)
    assert window.matches(_alert("db"), now=datetime(2024, 1, 1, 12, 0))
    assert window.matches(_alert("api"), now=datetime(2024, 1, 1, 12, 0))


def test_window_does_not_match_wrong_source():
    window = SuppressionWindow(source_name="db", alert_name=None)
    assert not window.matches(_alert("api"), now=datetime(2024, 1, 1, 12, 0))


def test_window_does_not_match_wrong_alert_name():
    window = SuppressionWindow(source_name=None, alert_name="latency")
    assert not window.matches(_alert(name="row_count"), now=datetime(2024, 1, 1, 12, 0))


def test_window_matches_within_time_range():
    window = SuppressionWindow(
        source_name=None, alert_name=None,
        start_time=time(22, 0), end_time=time(23, 59)
    )
    assert window.matches(_alert(), now=datetime(2024, 1, 1, 22, 30))


def test_window_does_not_match_outside_time_range():
    window = SuppressionWindow(
        source_name=None, alert_name=None,
        start_time=time(22, 0), end_time=time(23, 59)
    )
    assert not window.matches(_alert(), now=datetime(2024, 1, 1, 10, 0))


def test_window_matches_correct_weekday():
    # 2024-01-01 is a Monday (weekday=0)
    window = SuppressionWindow(source_name=None, alert_name=None, days=[0])
    assert window.matches(_alert(), now=datetime(2024, 1, 1, 12, 0))


def test_window_does_not_match_wrong_weekday():
    # 2024-01-02 is a Tuesday (weekday=1)
    window = SuppressionWindow(source_name=None, alert_name=None, days=[0])
    assert not window.matches(_alert(), now=datetime(2024, 1, 2, 12, 0))


def test_window_empty_days_matches_any_day():
    window = SuppressionWindow(source_name=None, alert_name=None, days=[])
    for day_offset in range(7):
        now = datetime(2024, 1, 1 + day_offset, 12, 0)
        assert window.matches(_alert(), now=now)


# ---------------------------------------------------------------------------
# Suppressor.evaluate
# ---------------------------------------------------------------------------

@pytest.fixture
def suppressor() -> Suppressor:
    return Suppressor()


def test_evaluate_returns_not_suppressed_with_no_windows(suppressor):
    result = suppressor.evaluate(_alert(), now=datetime(2024, 1, 1, 12, 0))
    assert isinstance(result, SuppressResult)
    assert not result.suppressed
    assert result.window is None


def test_evaluate_returns_suppressed_when_window_matches(suppressor):
    suppressor.add_window(SuppressionWindow(source_name="db", alert_name=None))
    result = suppressor.evaluate(_alert("db"), now=datetime(2024, 1, 1, 12, 0))
    assert result.suppressed
    assert result.window is not None


def test_evaluate_summary_suppressed(suppressor):
    suppressor.add_window(SuppressionWindow(source_name=None, alert_name=None))
    result = suppressor.evaluate(_alert(), now=datetime(2024, 1, 1, 12, 0))
    assert "SUPPRESSED" in result.summary


def test_evaluate_summary_allowed(suppressor):
    result = suppressor.evaluate(_alert(), now=datetime(2024, 1, 1, 12, 0))
    assert "ALLOWED" in result.summary


# ---------------------------------------------------------------------------
# Suppressor.filter
# ---------------------------------------------------------------------------

def test_filter_removes_suppressed_alerts(suppressor):
    suppressor.add_window(SuppressionWindow(source_name="db", alert_name=None))
    alerts = [_alert("db"), _alert("api")]
    result = suppressor.filter(alerts, now=datetime(2024, 1, 1, 12, 0))
    assert len(result) == 1
    assert result[0].source_name == "api"


def test_filter_returns_all_when_no_windows(suppressor):
    alerts = [_alert("db"), _alert("api")]
    result = suppressor.filter(alerts, now=datetime(2024, 1, 1, 12, 0))
    assert len(result) == 2


def test_filter_returns_empty_when_all_suppressed(suppressor):
    suppressor.add_window(SuppressionWindow(source_name=None, alert_name=None))
    alerts = [_alert("db"), _alert("api")]
    result = suppressor.filter(alerts, now=datetime(2024, 1, 1, 12, 0))
    assert result == []
