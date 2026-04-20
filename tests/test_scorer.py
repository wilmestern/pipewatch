"""Tests for pipewatch.scorer."""

from datetime import datetime, timedelta

import pytest

from pipewatch.config import SourceConfig, AlertConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.alerts import Alert
from pipewatch.history import SourceHistory
from pipewatch.scorer import Scorer, SourceScore, ScoreReport


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="pipe_a", query="SELECT 1", interval=60)


@pytest.fixture()
def alert_config() -> AlertConfig:
    return AlertConfig(name="row_count_low", threshold=10, operator="lt")


def _make_result(source_config: SourceConfig, healthy: bool) -> MetricResult:
    return MetricResult(
        source=source_config,
        metric=PipelineMetric(row_count=20 if healthy else 5, latency_ms=100),
        is_healthy=healthy,
        checked_at=datetime.utcnow(),
    )


@pytest.fixture()
def store(source_config: SourceConfig) -> SourceHistory:
    h = SourceHistory(max_entries=50)
    for healthy in [True, True, True, False]:
        h.record(source_config.name, _make_result(source_config, healthy))
    return h


@pytest.fixture()
def scorer(store: SourceHistory) -> Scorer:
    return Scorer(history=store)


def test_score_source_returns_source_score(scorer: Scorer) -> None:
    result = scorer.score_source("pipe_a", [])
    assert isinstance(result, SourceScore)
    assert result.source_name == "pipe_a"


def test_score_is_between_zero_and_one(scorer: Scorer) -> None:
    result = scorer.score_source("pipe_a", [])
    assert 0.0 <= result.score <= 1.0


def test_healthy_samples_counted_correctly(scorer: Scorer) -> None:
    result = scorer.score_source("pipe_a", [])
    assert result.total_samples == 4
    assert result.healthy_samples == 3


def test_active_alerts_reduce_score(scorer: Scorer, alert_config: AlertConfig) -> None:
    alert = Alert(source_name="pipe_a", config=alert_config, triggered_at=datetime.utcnow())
    without_alerts = scorer.score_source("pipe_a", [])
    with_alerts = scorer.score_source("pipe_a", [alert, alert])
    assert with_alerts.score < without_alerts.score


def test_grade_a_for_perfect_score() -> None:
    s = SourceScore(source_name="x", score=1.0, total_samples=10, healthy_samples=10, active_alert_count=0)
    assert s.grade == "A"


def test_grade_f_for_zero_score() -> None:
    s = SourceScore(source_name="x", score=0.0, total_samples=10, healthy_samples=0, active_alert_count=5)
    assert s.grade == "F"


def test_summary_contains_source_name(scorer: Scorer) -> None:
    result = scorer.score_source("pipe_a", [])
    assert "pipe_a" in result.summary()


def test_compute_returns_score_report(scorer: Scorer) -> None:
    report = scorer.compute({})
    assert isinstance(report, ScoreReport)
    assert len(report.scores) == 1


def test_score_report_for_source(scorer: Scorer) -> None:
    report = scorer.compute({})
    entry = report.for_source("pipe_a")
    assert entry is not None
    assert entry.source_name == "pipe_a"


def test_score_report_for_unknown_source_returns_none(scorer: Scorer) -> None:
    report = scorer.compute({})
    assert report.for_source("nonexistent") is None


def test_average_score_empty_report() -> None:
    report = ScoreReport(scores=[])
    assert report.average_score == 0.0


def test_invalid_alert_weight_raises() -> None:
    store = SourceHistory(max_entries=10)
    with pytest.raises(ValueError):
        Scorer(history=store, alert_weight=1.5)


def test_score_zero_when_no_history() -> None:
    store = SourceHistory(max_entries=10)
    scorer = Scorer(history=store)
    result = scorer.score_source("empty_source", [])
    assert result.score == 0.0
    assert result.total_samples == 0
