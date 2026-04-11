"""Tests for pipewatch.filter module."""

import pytest
from pipewatch.filter import FilterCriteria, MetricFilter
from pipewatch.metrics import MetricResult


def _make_result(
    source_name: str = "src",
    is_healthy: bool = True,
    value: float = 10.0,
    error: str = None,
) -> MetricResult:
    return MetricResult(
        source_name=source_name,
        is_healthy=is_healthy,
        value=value,
        error=error,
    )


@pytest.fixture
def mixed_results():
    return [
        _make_result(source_name="alpha", is_healthy=True, value=5.0),
        _make_result(source_name="beta", is_healthy=False, value=95.0, error="timeout error"),
        _make_result(source_name="alpha", is_healthy=False, value=80.0, error="connection refused"),
        _make_result(source_name="gamma", is_healthy=True, value=20.0),
    ]


def test_no_criteria_returns_all(mixed_results):
    f = MetricFilter(FilterCriteria())
    assert f.apply(mixed_results) == mixed_results


def test_filter_by_source_name(mixed_results):
    f = MetricFilter(FilterCriteria(source_name="alpha"))
    result = f.apply(mixed_results)
    assert len(result) == 2
    assert all(r.source_name == "alpha" for r in result)


def test_filter_only_unhealthy(mixed_results):
    f = MetricFilter(FilterCriteria(only_unhealthy=True))
    result = f.apply(mixed_results)
    assert len(result) == 2
    assert all(not r.is_healthy for r in result)


def test_filter_min_value(mixed_results):
    f = MetricFilter(FilterCriteria(min_value=50.0))
    result = f.apply(mixed_results)
    assert len(result) == 2
    assert all(r.value >= 50.0 for r in result)


def test_filter_max_value(mixed_results):
    f = MetricFilter(FilterCriteria(max_value=20.0))
    result = f.apply(mixed_results)
    assert len(result) == 2
    assert all(r.value <= 20.0 for r in result)


def test_filter_error_contains(mixed_results):
    f = MetricFilter(FilterCriteria(error_contains="timeout"))
    result = f.apply(mixed_results)
    assert len(result) == 1
    assert result[0].source_name == "beta"


def test_combined_criteria(mixed_results):
    f = MetricFilter(FilterCriteria(source_name="alpha", only_unhealthy=True))
    result = f.apply(mixed_results)
    assert len(result) == 1
    assert result[0].source_name == "alpha"
    assert not result[0].is_healthy


def test_first_returns_first_match(mixed_results):
    f = MetricFilter(FilterCriteria(only_unhealthy=True))
    result = f.first(mixed_results)
    assert result is not None
    assert not result.is_healthy


def test_first_returns_none_when_no_match(mixed_results):
    f = MetricFilter(FilterCriteria(source_name="nonexistent"))
    result = f.first(mixed_results)
    assert result is None


def test_apply_empty_list():
    f = MetricFilter(FilterCriteria(only_unhealthy=True))
    assert f.apply([]) == []


def test_filter_min_and_max_value(mixed_results):
    """Test that both min_value and max_value can be combined to define a range."""
    f = MetricFilter(FilterCriteria(min_value=10.0, max_value=85.0))
    result = f.apply(mixed_results)
    assert len(result) == 2
    assert all(10.0 <= r.value <= 85.0 for r in result)
