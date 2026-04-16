"""Tests for pipewatch.profiler."""
import pytest
from pipewatch.profiler import ProfileEntry, ProfileReport, Profiler


@pytest.fixture
def profiler() -> Profiler:
    return Profiler(slow_threshold_seconds=2.0)


def test_record_returns_profile_entry(profiler):
    entry = profiler.record("source_a", 1.5)
    assert isinstance(entry, ProfileEntry)
    assert entry.source_name == "source_a"
    assert entry.duration_seconds == pytest.approx(1.5)


def test_record_stores_entry(profiler):
    profiler.record("source_a", 1.0)
    assert profiler.latest("source_a") is not None


def test_latest_returns_none_when_empty(profiler):
    assert profiler.latest("missing") is None


def test_latest_returns_most_recent(profiler):
    profiler.record("source_a", 0.5)
    profiler.record("source_a", 3.0)
    assert profiler.latest("source_a").duration_seconds == pytest.approx(3.0)


def test_is_slow_false_below_threshold(profiler):
    profiler.record("source_a", 1.0)
    assert profiler.is_slow("source_a") is False


def test_is_slow_true_above_threshold(profiler):
    profiler.record("source_a", 5.0)
    assert profiler.is_slow("source_a") is True


def test_is_slow_false_when_no_data(profiler):
    assert profiler.is_slow("unknown") is False


def test_report_returns_profile_report(profiler):
    profiler.record("source_a", 1.0)
    report = profiler.report()
    assert isinstance(report, ProfileReport)


def test_report_contains_latest_per_source(profiler):
    profiler.record("source_a", 0.5)
    profiler.record("source_a", 2.5)
    profiler.record("source_b", 1.0)
    report = profiler.report()
    assert report.total_sources == 2
    durations = {e.source_name: e.duration_seconds for e in report.entries}
    assert durations["source_a"] == pytest.approx(2.5)
    assert durations["source_b"] == pytest.approx(1.0)


def test_report_average_duration(profiler):
    profiler.record("a", 1.0)
    profiler.record("b", 3.0)
    report = profiler.report()
    assert report.average_duration == pytest.approx(2.0)


def test_report_slowest(profiler):
    profiler.record("fast", 0.2)
    profiler.record("slow", 4.0)
    report = profiler.report()
    assert report.slowest.source_name == "slow"


def test_report_empty_when_no_data(profiler):
    report = profiler.report()
    assert report.total_sources == 0
    assert report.average_duration == pytest.approx(0.0)
    assert report.slowest is None


def test_clear_removes_all_entries(profiler):
    profiler.record("source_a", 1.0)
    profiler.clear()
    assert profiler.latest("source_a") is None


def test_entry_summary_format():
    entry = ProfileEntry(source_name="pipe_x", duration_seconds=1.234)
    assert "pipe_x" in entry.summary()
    assert "1.234" in entry.summary()


def test_report_summary_contains_source(profiler):
    profiler.record("source_a", 1.0)
    assert "source_a" in profiler.report().summary()
