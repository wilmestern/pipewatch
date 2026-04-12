"""Tests for pipewatch.checkpoint module."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.checkpoint import CheckpointEntry, CheckpointStore


FIXED_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def store() -> CheckpointStore:
    return CheckpointStore()


def test_get_returns_none_when_empty(store):
    assert store.get("source_a") is None


def test_update_creates_entry(store):
    entry = store.update("source_a", FIXED_NOW)
    assert isinstance(entry, CheckpointEntry)
    assert entry.source_name == "source_a"
    assert entry.last_seen == FIXED_NOW
    assert entry.run_count == 1


def test_update_increments_run_count(store):
    store.update("source_a", FIXED_NOW)
    entry = store.update("source_a", FIXED_NOW + timedelta(seconds=30))
    assert entry.run_count == 2


def test_get_returns_latest_entry(store):
    store.update("source_a", FIXED_NOW)
    later = FIXED_NOW + timedelta(minutes=5)
    store.update("source_a", later)
    assert store.get("source_a").last_seen == later


def test_age_seconds_returns_correct_delta():
    entry = CheckpointEntry(source_name="s", last_seen=FIXED_NOW)
    now = FIXED_NOW + timedelta(seconds=90)
    assert entry.age_seconds(now) == pytest.approx(90.0)


def test_is_stale_returns_true_when_no_checkpoint(store):
    assert store.is_stale("missing", max_age_seconds=60, now=FIXED_NOW) is True


def test_is_stale_returns_false_when_fresh(store):
    store.update("source_a", FIXED_NOW)
    now = FIXED_NOW + timedelta(seconds=30)
    assert store.is_stale("source_a", max_age_seconds=60, now=now) is False


def test_is_stale_returns_true_when_too_old(store):
    store.update("source_a", FIXED_NOW)
    now = FIXED_NOW + timedelta(seconds=120)
    assert store.is_stale("source_a", max_age_seconds=60, now=now) is True


def test_all_sources_returns_tracked_names(store):
    store.update("source_a", FIXED_NOW)
    store.update("source_b", FIXED_NOW)
    assert set(store.all_sources()) == {"source_a", "source_b"}


def test_clear_removes_entry(store):
    store.update("source_a", FIXED_NOW)
    store.clear("source_a")
    assert store.get("source_a") is None


def test_clear_nonexistent_does_not_raise(store):
    store.clear("nonexistent")  # should not raise


def test_update_without_timestamp_uses_current_time(store):
    entry = store.update("source_a")
    assert entry.last_seen.tzinfo is not None
    assert entry.run_count == 1
