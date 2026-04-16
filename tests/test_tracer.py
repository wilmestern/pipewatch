"""Tests for pipewatch.tracer."""
import time
import pytest
from pipewatch.tracer import Tracer, TraceSpan


@pytest.fixture
def tracer() -> Tracer:
    return Tracer()


def test_start_returns_trace_span(tracer):
    span = tracer.start("source_a")
    assert isinstance(span, TraceSpan)
    assert span.source_name == "source_a"
    assert span.ended_at is None
    assert span.success is None


def test_trace_id_is_unique(tracer):
    s1 = tracer.start("source_a")
    s2 = tracer.start("source_a")
    assert s1.trace_id != s2.trace_id


def test_finish_sets_ended_at(tracer):
    span = tracer.start("source_a")
    finished = tracer.finish(span, success=True)
    assert finished.ended_at is not None
    assert finished.success is True
    assert finished.error is None


def test_finish_records_error(tracer):
    span = tracer.start("source_a")
    tracer.finish(span, success=False, error="timeout")
    assert span.success is False
    assert span.error == "timeout"


def test_duration_seconds_after_finish(tracer):
    span = tracer.start("source_a")
    time.sleep(0.05)
    tracer.finish(span, success=True)
    assert span.duration_seconds is not None
    assert span.duration_seconds >= 0.04


def test_duration_none_before_finish(tracer):
    span = tracer.start("source_a")
    assert span.duration_seconds is None


def test_spans_for_returns_all(tracer):
    tracer.start("source_a")
    tracer.start("source_a")
    assert len(tracer.spans_for("source_a")) == 2


def test_spans_for_unknown_source_returns_empty(tracer):
    assert tracer.spans_for("ghost") == []


def test_latest_returns_most_recent(tracer):
    s1 = tracer.start("source_a")
    s2 = tracer.start("source_a")
    assert tracer.latest("source_a") is s2


def test_latest_returns_none_when_empty(tracer):
    assert tracer.latest("source_a") is None


def test_all_sources(tracer):
    tracer.start("source_a")
    tracer.start("source_b")
    assert set(tracer.all_sources()) == {"source_a", "source_b"}


def test_summary_ok(tracer):
    span = tracer.start("source_a")
    tracer.finish(span, success=True)
    assert "ok" in span.summary()
    assert "source_a" in span.summary()


def test_summary_error(tracer):
    span = tracer.start("source_a")
    tracer.finish(span, success=False, error="boom")
    assert "error" in span.summary()


def test_summary_running(tracer):
    span = tracer.start("source_a")
    assert "running" in span.summary()
