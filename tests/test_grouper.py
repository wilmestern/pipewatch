"""Tests for pipewatch.grouper."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from pipewatch.grouper import Grouper, MetricGroup
from pipewatch.metrics import MetricResult


def _make_result(source: str, healthy: bool, value: float = 1.0) -> MetricResult:
    metric = MagicMock()
    metric.value = value
    return MetricResult(
        source_name=source,
        metric=metric,
        is_healthy=healthy,
        collected_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        error=None,
    )


@pytest.fixture
def mixed_results():
    return [
        _make_result("db", True),
        _make_result("db", False),
        _make_result("api", True),
        _make_result("queue", False),
    ]


@pytest.fixture
def grouper():
    return Grouper()


def test_group_by_source_name_produces_correct_keys(grouper, mixed_results):
    groups = grouper.group(mixed_results)
    assert set(groups.keys()) == {"db", "api", "queue"}


def test_group_counts_match_input(grouper, mixed_results):
    groups = grouper.group(mixed_results)
    assert len(groups["db"].results) == 2
    assert len(groups["api"].results) == 1
    assert len(groups["queue"].results) == 1


def test_healthy_count_and_unhealthy_count(grouper, mixed_results):
    groups = grouper.group(mixed_results)
    assert groups["db"].healthy_count == 1
    assert groups["db"].unhealthy_count == 1


def test_group_is_healthy_when_all_healthy(grouper):
    results = [_make_result("api", True), _make_result("api", True)]
    groups = grouper.group(results)
    assert groups["api"].is_healthy is True


def test_group_is_unhealthy_when_any_unhealthy(grouper, mixed_results):
    groups = grouper.group(mixed_results)
    assert groups["db"].is_healthy is False


def test_unhealthy_groups_excludes_fully_healthy(grouper, mixed_results):
    unhealthy = grouper.unhealthy_groups(mixed_results)
    keys = [g.key for g in unhealthy]
    assert "api" not in keys
    assert "db" in keys
    assert "queue" in keys


def test_empty_results_returns_empty_dict(grouper):
    assert grouper.group([]) == {}


def test_custom_key_fn_groups_by_health_status():
    grouper = Grouper(key_fn=lambda r: "ok" if r.is_healthy else "fail")
    results = [
        _make_result("a", True),
        _make_result("b", False),
        _make_result("c", True),
    ]
    groups = grouper.group(results)
    assert len(groups["ok"].results) == 2
    assert len(groups["fail"].results) == 1


def test_summary_contains_key_and_counts():
    group = MetricGroup(key="db", results=[
        _make_result("db", True),
        _make_result("db", False),
    ])
    s = group.summary()
    assert "db" in s
    assert "2 result" in s
    assert "1 healthy" in s
    assert "1 unhealthy" in s
