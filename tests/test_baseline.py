"""Tests for pipewatch.baseline."""

from __future__ import annotations

import pytest

from pipewatch.baseline import BaselineEntry, BaselineResult, BaselineTracker
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="orders", type="sql", query="SELECT COUNT(*) FROM orders")


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(metric="row_count", min_value=0, max_value=1000)


def _make_result(value: float, source_name: str = "orders") -> MetricResult:
    metric = PipelineMetric(name="row_count", value=value)
    return MetricResult(
        source_name=source_name,
        metric=metric,
        success=True,
        error=None,
    )


@pytest.fixture()
def tracker() -> BaselineTracker:
    t = BaselineTracker()
    t.register(BaselineEntry(source_name="orders", expected_value=500.0, tolerance=0.10))
    return t


# ---------------------------------------------------------------------------
# BaselineEntry tests
# ---------------------------------------------------------------------------

def test_within_baseline_returns_true_for_exact_match():
    entry = BaselineEntry(source_name="s", expected_value=100.0, tolerance=0.10)
    assert entry.within_baseline(100.0) is True


def test_within_baseline_returns_true_at_upper_bound():
    entry = BaselineEntry(source_name="s", expected_value=100.0, tolerance=0.10)
    assert entry.within_baseline(110.0) is True


def test_within_baseline_returns_false_above_upper_bound():
    entry = BaselineEntry(source_name="s", expected_value=100.0, tolerance=0.10)
    assert entry.within_baseline(110.1) is False


def test_within_baseline_returns_false_below_lower_bound():
    entry = BaselineEntry(source_name="s", expected_value=100.0, tolerance=0.10)
    assert entry.within_baseline(89.9) is False


# ---------------------------------------------------------------------------
# BaselineTracker.compare tests
# ---------------------------------------------------------------------------

def test_compare_returns_none_when_no_baseline_registered():
    tracker = BaselineTracker()
    result = tracker.compare(_make_result(500.0))
    assert result is None


def test_compare_returns_baseline_result(tracker: BaselineTracker):
    br = tracker.compare(_make_result(500.0))
    assert isinstance(br, BaselineResult)


def test_compare_within_baseline_for_normal_value(tracker: BaselineTracker):
    br = tracker.compare(_make_result(510.0))  # 2 % above, within 10 %
    assert br is not None
    assert br.within_baseline is True


def test_compare_anomaly_for_outlier_value(tracker: BaselineTracker):
    br = tracker.compare(_make_result(600.0))  # 20 % above
    assert br is not None
    assert br.within_baseline is False


def test_compare_deviation_pct_positive_when_above(tracker: BaselineTracker):
    br = tracker.compare(_make_result(550.0))
    assert br is not None
    assert br.deviation_pct == pytest.approx(10.0)


def test_compare_deviation_pct_negative_when_below(tracker: BaselineTracker):
    br = tracker.compare(_make_result(450.0))
    assert br is not None
    assert br.deviation_pct == pytest.approx(-10.0)


def test_summary_contains_anomaly_label_for_outlier(tracker: BaselineTracker):
    br = tracker.compare(_make_result(700.0))
    assert br is not None
    assert "ANOMALY" in br.summary


def test_summary_contains_ok_label_for_normal(tracker: BaselineTracker):
    br = tracker.compare(_make_result(500.0))
    assert br is not None
    assert "OK" in br.summary


# ---------------------------------------------------------------------------
# BaselineTracker.compare_all tests
# ---------------------------------------------------------------------------

def test_compare_all_skips_sources_without_baseline(tracker: BaselineTracker):
    results = [
        _make_result(500.0, source_name="orders"),
        _make_result(200.0, source_name="unknown_source"),
    ]
    out = tracker.compare_all(results)
    assert len(out) == 1
    assert out[0].source_name == "orders"


def test_compare_all_returns_empty_list_when_no_baselines():
    tracker = BaselineTracker()
    results = [_make_result(500.0)]
    assert tracker.compare_all(results) == []
