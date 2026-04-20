"""Tests for pipewatch.reaper."""

from datetime import datetime, timedelta

import pytest

from pipewatch.checkpoint import CheckpointStore
from pipewatch.reaper import Reaper, ReaperPolicy, ReapResult


@pytest.fixture
def store() -> CheckpointStore:
    return CheckpointStore()


@pytest.fixture
def policy() -> ReaperPolicy:
    return ReaperPolicy(max_inactivity_seconds=300)


@pytest.fixture
def reaper(policy: ReaperPolicy, store: CheckpointStore) -> Reaper:
    return Reaper(policy=policy, store=store)


def test_policy_rejects_non_positive_inactivity() -> None:
    with pytest.raises(ValueError, match="max_inactivity_seconds must be positive"):
        ReaperPolicy(max_inactivity_seconds=0)


def test_run_returns_empty_when_no_sources(reaper: Reaper) -> None:
    results = reaper.run()
    assert results == []


def test_active_source_is_not_reaped(reaper: Reaper, store: CheckpointStore) -> None:
    now = datetime.utcnow()
    store.update("active-source", now)
    results = reaper.run(now=now)
    assert results == []


def test_stale_source_is_reaped(reaper: Reaper, store: CheckpointStore) -> None:
    now = datetime.utcnow()
    stale_time = now - timedelta(seconds=400)
    store.update("stale-source", stale_time)
    results = reaper.run(now=now)
    assert len(results) == 1
    assert results[0].source_name == "stale-source"


def test_reaped_source_is_removed_from_store(reaper: Reaper, store: CheckpointStore) -> None:
    now = datetime.utcnow()
    stale_time = now - timedelta(seconds=400)
    store.update("stale-source", stale_time)
    reaper.run(now=now)
    assert store.get("stale-source") is None


def test_dry_run_does_not_remove_source(store: CheckpointStore) -> None:
    policy = ReaperPolicy(max_inactivity_seconds=300, dry_run=True)
    reaper = Reaper(policy=policy, store=store)
    now = datetime.utcnow()
    stale_time = now - timedelta(seconds=400)
    store.update("stale-source", stale_time)
    results = reaper.run(now=now)
    assert len(results) == 1
    assert results[0].dry_run is True
    assert store.get("stale-source") is not None


def test_reap_result_summary_includes_source_name() -> None:
    now = datetime.utcnow()
    result = ReapResult(
        source_name="my-pipeline",
        last_seen=now - timedelta(seconds=60),
        reaped_at=now,
        dry_run=False,
    )
    assert "my-pipeline" in result.summary()


def test_dry_run_summary_has_prefix() -> None:
    now = datetime.utcnow()
    result = ReapResult(
        source_name="my-pipeline",
        last_seen=now - timedelta(seconds=60),
        reaped_at=now,
        dry_run=True,
    )
    assert result.summary().startswith("[DRY RUN]")


def test_reaped_sources_accumulates_across_runs(store: CheckpointStore, policy: ReaperPolicy) -> None:
    reaper = Reaper(policy=policy, store=store)
    now = datetime.utcnow()
    store.update("source-a", now - timedelta(seconds=400))
    reaper.run(now=now)
    store.update("source-b", now - timedelta(seconds=500))
    reaper.run(now=now)
    assert len(reaper.reaped_sources()) == 2


def test_only_stale_sources_reaped_when_mixed(reaper: Reaper, store: CheckpointStore) -> None:
    now = datetime.utcnow()
    store.update("fresh", now - timedelta(seconds=10))
    store.update("stale", now - timedelta(seconds=600))
    results = reaper.run(now=now)
    assert len(results) == 1
    assert results[0].source_name == "stale"
