"""Tests for pipewatch configuration loading."""

import os
import textwrap
import pytest

from pipewatch.config import load_config, PipewatchConfig, SourceConfig, AlertConfig


@pytest.fixture
def config_file(tmp_path):
    """Write a sample config YAML and return its path."""
    content = textwrap.dedent("""\
        poll_interval_seconds: 120
        log_level: DEBUG
        sources:
          - name: prod-postgres
            type: postgres
            connection:
              host: db.example.com
              port: 5432
              dbname: analytics
            tags: [production, critical]
          - name: s3-raw
            type: s3
            connection:
              bucket: my-data-bucket
        alerts:
          - channel: slack
            threshold_minutes: 30
            enabled: true
          - channel: email
            threshold_minutes: 90
    """)
    p = tmp_path / "config.yaml"
    p.write_text(content)
    return str(p)


def test_load_config_returns_pipewatch_config(config_file):
    cfg = load_config(config_file)
    assert isinstance(cfg, PipewatchConfig)


def test_load_config_poll_interval(config_file):
    cfg = load_config(config_file)
    assert cfg.poll_interval_seconds == 120


def test_load_config_log_level(config_file):
    cfg = load_config(config_file)
    assert cfg.log_level == "DEBUG"


def test_load_config_sources(config_file):
    cfg = load_config(config_file)
    assert len(cfg.sources) == 2
    assert isinstance(cfg.sources[0], SourceConfig)
    assert cfg.sources[0].name == "prod-postgres"
    assert cfg.sources[0].type == "postgres"
    assert cfg.sources[0].connection["host"] == "db.example.com"
    assert "production" in cfg.sources[0].tags


def test_load_config_alerts(config_file):
    cfg = load_config(config_file)
    assert len(cfg.alerts) == 2
    assert isinstance(cfg.alerts[0], AlertConfig)
    assert cfg.alerts[0].channel == "slack"
    assert cfg.alerts[0].threshold_minutes == 30
    assert cfg.alerts[1].enabled is True  # default


def test_load_config_defaults(tmp_path):
    p = tmp_path / "minimal.yaml"
    p.write_text("{}\n")
    cfg = load_config(str(p))
    assert cfg.poll_interval_seconds == 300
    assert cfg.log_level == "INFO"
    assert cfg.sources == []
    assert cfg.alerts == []


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("/nonexistent/path/config.yaml")
