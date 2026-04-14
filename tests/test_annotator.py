"""Tests for pipewatch/annotator.py."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipewatch.annotator import Annotation, AnnotatedResult, Annotator
from pipewatch.config import AlertConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric


@pytest.fixture()
def source_config() -> SourceConfig:
    return SourceConfig(name="db", type="postgres", connection_string="postgresql://localhost/db")


@pytest.fixture()
def metric_result(source_config: SourceConfig) -> MetricResult:
    metric = PipelineMetric(row_count=100, latency_seconds=0.5, error_rate=0.0)
    return MetricResult(source_name=source_config.name, metric=metric, success=True)


@pytest.fixture()
def annotator() -> Annotator:
    return Annotator()


def test_add_returns_annotation(annotator: Annotator) -> None:
    ann = annotator.add("db", "looks fine", "alice")
    assert isinstance(ann, Annotation)
    assert ann.note == "looks fine"
    assert ann.author == "alice"
    assert ann.source_name == "db"


def test_get_returns_empty_list_for_unknown_source(annotator: Annotator) -> None:
    assert annotator.get("unknown") == []


def test_get_returns_all_annotations_for_source(annotator: Annotator) -> None:
    annotator.add("db", "note one", "alice")
    annotator.add("db", "note two", "bob")
    result = annotator.get("db")
    assert len(result) == 2
    assert {r.note for r in result} == {"note one", "note two"}


def test_get_does_not_mix_sources(annotator: Annotator) -> None:
    annotator.add("db", "db note", "alice")
    annotator.add("kafka", "kafka note", "bob")
    assert len(annotator.get("db")) == 1
    assert len(annotator.get("kafka")) == 1


def test_annotate_result_attaches_notes(annotator: Annotator, metric_result: MetricResult) -> None:
    annotator.add("db", "checked manually", "alice")
    ar = annotator.annotate_result(metric_result)
    assert isinstance(ar, AnnotatedResult)
    assert ar.has_annotations()
    assert ar.annotations[0].note == "checked manually"


def test_annotate_result_no_notes(annotator: Annotator, metric_result: MetricResult) -> None:
    ar = annotator.annotate_result(metric_result)
    assert not ar.has_annotations()


def test_latest_annotation_returns_most_recent(annotator: Annotator) -> None:
    ann1 = annotator.add("db", "first", "alice")
    ann2 = annotator.add("db", "second", "bob")
    # Force ordering
    ann1.created_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ann2.created_at = datetime(2024, 6, 1, tzinfo=timezone.utc)
    ar = AnnotatedResult(result=None, annotations=[ann1, ann2])  # type: ignore[arg-type]
    assert ar.latest_annotation() is ann2


def test_latest_annotation_returns_none_when_empty() -> None:
    ar = AnnotatedResult(result=None, annotations=[])  # type: ignore[arg-type]
    assert ar.latest_annotation() is None


def test_annotation_summary_format() -> None:
    ann = Annotation(
        source_name="db",
        note="all good",
        author="carol",
        created_at=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
    )
    assert ann.summary() == "[2024-03-15T12:00:00Z] carol on 'db': all good"


def test_clear_removes_annotations(annotator: Annotator) -> None:
    annotator.add("db", "note", "alice")
    removed = annotator.clear("db")
    assert removed == 1
    assert annotator.get("db") == []


def test_clear_returns_zero_for_unknown_source(annotator: Annotator) -> None:
    assert annotator.clear("nonexistent") == 0
