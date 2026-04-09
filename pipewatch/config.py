"""Configuration loading and validation for pipewatch."""

import os
from dataclasses import dataclass, field
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = os.path.expanduser("~/.pipewatch/config.yaml")


@dataclass
class SourceConfig:
    name: str
    type: str
    connection: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class AlertConfig:
    channel: str
    threshold_minutes: int = 60
    enabled: bool = True


@dataclass
class PipewatchConfig:
    sources: list[SourceConfig] = field(default_factory=list)
    alerts: list[AlertConfig] = field(default_factory=list)
    poll_interval_seconds: int = 300
    log_level: str = "INFO"


def load_config(path: str = DEFAULT_CONFIG_PATH) -> PipewatchConfig:
    """Load and parse the pipewatch configuration file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f) or {}

    sources = [
        SourceConfig(
            name=s["name"],
            type=s["type"],
            connection=s.get("connection", {}),
            tags=s.get("tags", []),
        )
        for s in raw.get("sources", [])
    ]

    alerts = [
        AlertConfig(
            channel=a["channel"],
            threshold_minutes=a.get("threshold_minutes", 60),
            enabled=a.get("enabled", True),
        )
        for a in raw.get("alerts", [])
    ]

    return PipewatchConfig(
        sources=sources,
        alerts=alerts,
        poll_interval_seconds=raw.get("poll_interval_seconds", 300),
        log_level=raw.get("log_level", "INFO"),
    )
