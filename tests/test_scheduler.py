"""Tests for pipewatch.scheduler."""

from unittest.mock import MagicMock, call, patch

import pytest

from pipewatch.config import AlertConfig, PipewatchConfig, SourceConfig
from pipewatch.metrics import MetricResult, PipelineMetric
from pipewatch.scheduler import Scheduler


@pytest.fixture()
def source_config():
    return SourceConfig(name="db", type="postgres", connection="postgresql://localhost/db")


@pytest.fixture()
def alert_config():
    return AlertConfig(metric="row_count", threshold=100, condition="below")


@pytest.fixture()
def pw_config(source_config, alert_config):
    return PipewatchConfig(
        sources=[source_config],
        alerts=[alert_config],
        poll_interval=30,
        log_level="INFO",
    )


@pytest.fixture()
def metric_result(source_config):
    metric = PipelineMetric(name="row_count", value=200.0, source=source_config.name)
    return MetricResult(source=source_config.name, metrics=[metric], errors=[])


@pytest.fixture()
def scheduler(pw_config):
    collector = MagicMock()
    evaluator = MagicMock()
    reporter = MagicMock()
    sleep_fn = MagicMock()
    return Scheduler(
        config=pw_config,
        collector=collector,
        evaluator=evaluator,
        reporter=reporter,
        sleep_fn=sleep_fn,
    )


def test_run_once_calls_collect_for_each_source(scheduler, source_config, metric_result):
    scheduler.collector.collect.return_value = metric_result
    scheduler.run_once()
    scheduler.collector.collect.assert_called_once_with(source_config)


def test_run_once_calls_evaluate_with_result(scheduler, metric_result):
    scheduler.collector.collect.return_value = metric_result
    scheduler.run_once()
    scheduler.evaluator.evaluate.assert_called_once_with(metric_result)


def test_run_once_calls_reporter_emit(scheduler, source_config, metric_result):
    scheduler.collector.collect.return_value = metric_result
    scheduler.run_once()
    assert scheduler.reporter.emit.call_count == 1


def test_run_once_skips_none_result(scheduler):
    scheduler.collector.collect.return_value = None
    scheduler.run_once()
    scheduler.evaluator.evaluate.assert_not_called()
    scheduler.reporter.emit.assert_not_called()


def test_start_calls_run_once_then_sleep(scheduler, metric_result):
    scheduler.collector.collect.return_value = metric_result
    call_count = 0

    def fake_sleep(interval):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            scheduler.stop()

    scheduler._sleep = fake_sleep
    scheduler.start()
    assert call_count == 2


def test_stop_sets_running_false(scheduler):
    scheduler._running = True
    scheduler.stop()
    assert scheduler._running is False
