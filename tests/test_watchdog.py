"""Tests for pipewatch.watchdog."""

from datetime import datetime, timedelta

import pytest

from pipewatch.checkpoint import CheckpointStore
from pipewatch.watchdog import Watchdog, WatchdogAlert, WatchdogRule


NOW = datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture
def store() -> CheckpointStore:
    return CheckpointStore()


@pytest.fixture
def rule() -> WatchdogRule:
    return WatchdogRule(source_name="db_pipeline", max_silence_seconds=60.0)


@pytest.fixture
def watchdog(rule: WatchdogRule, store: CheckpointStore) -> Watchdog:
    return Watchdog(rules=[rule], store=store)


# --- WatchdogRule ---

def test_rule_not_stale_when_recent(rule: WatchdogRule) -> None:
    last_seen = NOW - timedelta(seconds=30)
    assert rule.is_stale(last_seen, now=NOW) is False


def test_rule_stale_when_exceeded(rule: WatchdogRule) -> None:
    last_seen = NOW - timedelta(seconds=90)
    assert rule.is_stale(last_seen, now=NOW) is True


def test_rule_not_stale_at_exact_boundary(rule: WatchdogRule) -> None:
    last_seen = NOW - timedelta(seconds=60)
    assert rule.is_stale(last_seen, now=NOW) is False


# --- WatchdogAlert ---

def test_alert_summary_contains_source_name() -> None:
    alert = WatchdogAlert(
        source_name="my_source",
        last_seen=NOW,
        silence_seconds=120.0,
    )
    assert "my_source" in alert.summary()


def test_alert_summary_contains_silence_duration() -> None:
    alert = WatchdogAlert(
        source_name="my_source",
        last_seen=NOW,
        silence_seconds=120.0,
    )
    assert "120.0" in alert.summary()


# --- Watchdog.check ---

def test_no_alerts_when_store_is_empty(watchdog: Watchdog) -> None:
    alerts = watchdog.check(now=NOW)
    assert alerts == []


def test_no_alert_when_source_is_recent(watchdog: Watchdog, store: CheckpointStore) -> None:
    store.update("db_pipeline", value=1.0)
    # Patch last_updated to be recent
    store._entries["db_pipeline"].last_updated = NOW - timedelta(seconds=10)
    alerts = watchdog.check(now=NOW)
    assert alerts == []


def test_alert_raised_when_source_is_stale(watchdog: Watchdog, store: CheckpointStore) -> None:
    store.update("db_pipeline", value=1.0)
    store._entries["db_pipeline"].last_updated = NOW - timedelta(seconds=120)
    alerts = watchdog.check(now=NOW)
    assert len(alerts) == 1
    assert alerts[0].source_name == "db_pipeline"
    assert alerts[0].silence_seconds == pytest.approx(120.0)


def test_add_and_check_new_rule(watchdog: Watchdog, store: CheckpointStore) -> None:
    watchdog.add_rule(WatchdogRule(source_name="api_pipeline", max_silence_seconds=30.0))
    store.update("api_pipeline", value=1.0)
    store._entries["api_pipeline"].last_updated = NOW - timedelta(seconds=60)
    alerts = watchdog.check(now=NOW)
    sources = [a.source_name for a in alerts]
    assert "api_pipeline" in sources


def test_remove_rule_stops_alerting(watchdog: Watchdog, store: CheckpointStore) -> None:
    store.update("db_pipeline", value=1.0)
    store._entries["db_pipeline"].last_updated = NOW - timedelta(seconds=120)
    watchdog.remove_rule("db_pipeline")
    alerts = watchdog.check(now=NOW)
    assert alerts == []
