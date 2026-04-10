"""Tests for trend analysis module."""
import pytest
from unittest.mock import MagicMock
from datetime import datetime

from pipewatch.trend import TrendAnalyzer, TrendDirection, TrendResult
from pipewatch.history import SourceHistory, MetricSnapshot
from pipewatch.metrics import PipelineMetric, MetricResult


def _make_result(source: str, healthy: bool, latency: float = 1.0) -> MetricResult:
    metric = PipelineMetric(
        source_name=source,
        row_count=100,
        latency_seconds=latency,
        error_count=0 if healthy else 5,
        collected_at=datetime.utcnow(),
    )
    return MetricResult(source_name=source, metric=metric, healthy=healthy, errors=[])


@pytest.fixture
def store():
    return SourceHistory()


@pytest.fixture
def analyzer(store):
    return TrendAnalyzer(store, window=6)


def test_insufficient_data_when_empty(analyzer):
    result = analyzer.analyze("src_a")
    assert result.direction == TrendDirection.INSUFFICIENT_DATA
    assert result.sample_count == 0


def test_insufficient_data_below_min_samples(store, analyzer):
    store.record(_make_result("src_a", True))
    store.record(_make_result("src_a", False))
    result = analyzer.analyze("src_a")
    assert result.direction == TrendDirection.INSUFFICIENT_DATA
    assert result.sample_count == 2


def test_stable_trend_all_healthy(store, analyzer):
    for _ in range(6):
        store.record(_make_result("src_b", True))
    result = analyzer.analyze("src_b")
    assert result.direction == TrendDirection.STABLE
    assert result.error_rate == 0.0


def test_degrading_trend(store, analyzer):
    for _ in range(3):
        store.record(_make_result("src_c", True))
    for _ in range(3):
        store.record(_make_result("src_c", False))
    result = analyzer.analyze("src_c")
    assert result.direction == TrendDirection.DEGRADING


def test_improving_trend(store, analyzer):
    for _ in range(3):
        store.record(_make_result("src_d", False))
    for _ in range(3):
        store.record(_make_result("src_d", True))
    result = analyzer.analyze("src_d")
    assert result.direction == TrendDirection.IMPROVING


def test_average_latency_computed(store, analyzer):
    for lat in [1.0, 2.0, 3.0, 4.0]:
        store.record(_make_result("src_e", True, latency=lat))
    result = analyzer.analyze("src_e")
    assert result.average_latency == pytest.approx(2.5)


def test_summary_contains_source_name(store, analyzer):
    for _ in range(4):
        store.record(_make_result("src_f", True))
    result = analyzer.analyze("src_f")
    assert "src_f" in result.summary


def test_summary_insufficient_data(analyzer):
    result = analyzer.analyze("missing_src")
    assert "insufficient data" in result.summary
