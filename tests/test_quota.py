"""Tests for pipewatch.quota."""
from datetime import datetime, timedelta
import pytest
from pipewatch.quota import QuotaRule, QuotaState, QuotaManager


@pytest.fixture
def rule() -> QuotaRule:
    return QuotaRule(source_name="db_pipeline", max_runs=3, window_seconds=60)


@pytest.fixture
def manager(rule: QuotaRule) -> QuotaManager:
    return QuotaManager(rules=[rule])


def test_check_allowed_before_any_runs(manager):
    result = manager.check("db_pipeline")
    assert result.allowed is True
    assert result.current_count == 0


def test_check_allowed_for_unknown_source(manager):
    result = manager.check("unknown_source")
    assert result.allowed is True


def test_check_and_record_increments_count(manager):
    manager.check_and_record("db_pipeline")
    manager.check_and_record("db_pipeline")
    result = manager.check("db_pipeline")
    assert result.current_count == 2


def test_denied_after_max_runs_reached(manager):
    for _ in range(3):
        manager.check_and_record("db_pipeline")
    result = manager.check("db_pipeline")
    assert result.allowed is False
    assert result.current_count == 3


def test_check_and_record_does_not_record_when_denied(manager):
    for _ in range(3):
        manager.check_and_record("db_pipeline")
    manager.check_and_record("db_pipeline")  # denied, should not record
    result = manager.check("db_pipeline")
    assert result.current_count == 3


def test_wildcard_rule_applies_to_all_sources():
    wildcard_rule = QuotaRule(source_name=None, max_runs=2, window_seconds=60)
    mgr = QuotaManager(rules=[wildcard_rule])
    mgr.check_and_record("source_a")
    mgr.check_and_record("source_a")
    result = mgr.check("source_a")
    assert result.allowed is False


def test_specific_rule_takes_priority_over_wildcard():
    wildcard = QuotaRule(source_name=None, max_runs=1, window_seconds=60)
    specific = QuotaRule(source_name="special", max_runs=10, window_seconds=60)
    mgr = QuotaManager(rules=[specific, wildcard])
    for _ in range(5):
        mgr.check_and_record("special")
    result = mgr.check("special")
    assert result.allowed is True
    assert result.current_count == 5


def test_quota_state_prune_removes_old_entries():
    state = QuotaState()
    state.timestamps.append(datetime.utcnow() - timedelta(seconds=120))
    state.timestamps.append(datetime.utcnow())
    count = state.count_in_window(timedelta(seconds=60))
    assert count == 1


def test_result_summary_allowed():
    from pipewatch.quota import QuotaResult
    r = QuotaResult("my_pipe", True, 1, 5, 60)
    assert "allowed" in r.summary()
    assert "my_pipe" in r.summary()


def test_result_summary_denied():
    from pipewatch.quota import QuotaResult
    r = QuotaResult("my_pipe", False, 5, 5, 60)
    assert "denied" in r.summary()
