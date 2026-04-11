"""Tests for pipewatch.escalation."""

from datetime import datetime, timedelta

import pytest

from pipewatch.alerts import Alert
from pipewatch.escalation import EscalationManager, EscalationRule


def _alert(source: str = "db", name: str = "row_count") -> Alert:
    return Alert(source_name=source, alert_name=name, message="threshold exceeded")


@pytest.fixture
def rule() -> EscalationRule:
    return EscalationRule(
        source_name=None,
        alert_name=None,
        escalate_after_seconds=60,
        max_escalations=2,
    )


@pytest.fixture
def manager(rule: EscalationRule) -> EscalationManager:
    return EscalationManager(rules=[rule])


def test_first_evaluation_does_not_escalate(manager: EscalationManager) -> None:
    alerts = [_alert()]
    result = manager.evaluate(alerts)
    assert result == []


def test_escalates_after_threshold(manager: EscalationManager) -> None:
    alert = _alert()
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    manager.evaluate([alert], now=t0)
    t1 = t0 + timedelta(seconds=61)
    result = manager.evaluate([alert], now=t1)
    assert len(result) == 1
    assert result[0].source_name == "db"


def test_does_not_escalate_before_threshold(manager: EscalationManager) -> None:
    alert = _alert()
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    manager.evaluate([alert], now=t0)
    t1 = t0 + timedelta(seconds=30)
    result = manager.evaluate([alert], now=t1)
    assert result == []


def test_respects_max_escalations(manager: EscalationManager) -> None:
    alert = _alert()
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    manager.evaluate([alert], now=t0)
    for i in range(5):
        t = t0 + timedelta(seconds=61 * (i + 1))
        manager.evaluate([alert], now=t)
    # max_escalations=2, so only 2 escalations total
    state = manager._states["db:row_count"]
    assert state.escalation_count == 2


def test_state_cleared_when_alert_resolves(manager: EscalationManager) -> None:
    alert = _alert()
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    manager.evaluate([alert], now=t0)
    assert "db:row_count" in manager._states
    manager.evaluate([], now=t0 + timedelta(seconds=10))
    assert "db:row_count" not in manager._states


def test_rule_matches_specific_source_only() -> None:
    rule = EscalationRule(source_name="db", alert_name=None, escalate_after_seconds=60)
    mgr = EscalationManager(rules=[rule])
    other_alert = _alert(source="api", name="latency")
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    mgr.evaluate([other_alert], now=t0)
    t1 = t0 + timedelta(seconds=120)
    result = mgr.evaluate([other_alert], now=t1)
    assert result == []


def test_reset_clears_state(manager: EscalationManager) -> None:
    alert = _alert()
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    manager.evaluate([alert], now=t0)
    manager.reset("db", "row_count")
    assert "db:row_count" not in manager._states


def test_multiple_alerts_tracked_independently(manager: EscalationManager) -> None:
    a1 = _alert(source="db", name="row_count")
    a2 = _alert(source="api", name="latency")
    t0 = datetime(2024, 1, 1, 12, 0, 0)
    manager.evaluate([a1, a2], now=t0)
    t1 = t0 + timedelta(seconds=90)
    result = manager.evaluate([a1, a2], now=t1)
    assert len(result) == 2
    sources = {r.source_name for r in result}
    assert sources == {"db", "api"}
