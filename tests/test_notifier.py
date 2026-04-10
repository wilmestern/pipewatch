"""Tests for pipewatch.notifier."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import Alert
from pipewatch.notifier import EmailNotifier, LogNotifier, build_notifier


@pytest.fixture()
def sample_alerts() -> list[Alert]:
    return [
        Alert(
            source_name="warehouse",
            metric_name="row_count",
            current_value=5,
            threshold=10,
            message="row_count 5 is below threshold 10",
        ),
        Alert(
            source_name="api",
            metric_name="error_rate",
            current_value=0.12,
            threshold=0.05,
            message="error_rate 0.12 exceeds threshold 0.05",
        ),
    ]


# ---------------------------------------------------------------------------
# LogNotifier
# ---------------------------------------------------------------------------

def test_log_notifier_returns_true(sample_alerts):
    notifier = LogNotifier(level="WARNING")
    assert notifier.send(sample_alerts) is True


def test_log_notifier_empty_list():
    notifier = LogNotifier()
    assert notifier.send([]) is True


def test_log_notifier_writes_to_log(sample_alerts, caplog):
    notifier = LogNotifier(level="warning")
    with caplog.at_level(logging.WARNING, logger="pipewatch.notifier"):
        notifier.send(sample_alerts)
    assert "warehouse" in caplog.text
    assert "row_count" in caplog.text


# ---------------------------------------------------------------------------
# EmailNotifier
# ---------------------------------------------------------------------------

def test_email_notifier_sends_message(sample_alerts):
    notifier = EmailNotifier(
        smtp_host="localhost",
        smtp_port=25,
        sender="pipewatch@example.com",
        recipients=["ops@example.com"],
    )
    mock_server = MagicMock()
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp_cls.return_value.__enter__ = lambda s: mock_server
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        result = notifier.send(sample_alerts)
    assert result is True


def test_email_notifier_returns_false_on_error(sample_alerts):
    notifier = EmailNotifier(
        smtp_host="bad-host",
        smtp_port=9999,
        sender="a@b.com",
        recipients=["c@d.com"],
    )
    with patch("smtplib.SMTP", side_effect=OSError("connection refused")):
        result = notifier.send(sample_alerts)
    assert result is False


def test_email_notifier_empty_alerts():
    notifier = EmailNotifier(
        smtp_host="localhost",
        smtp_port=25,
        sender="a@b.com",
        recipients=["c@d.com"],
    )
    assert notifier.send([]) is True


# ---------------------------------------------------------------------------
# build_notifier factory
# ---------------------------------------------------------------------------

def test_build_notifier_default_is_log():
    notifier = build_notifier({})
    assert isinstance(notifier, LogNotifier)


def test_build_notifier_log_level():
    notifier = build_notifier({"backend": "log", "level": "ERROR"})
    assert isinstance(notifier, LogNotifier)
    assert notifier.level == "ERROR"


def test_build_notifier_email():
    cfg = {
        "backend": "email",
        "smtp_host": "smtp.example.com",
        "smtp_port": "587",
        "sender": "pw@example.com",
        "recipients": ["team@example.com"],
    }
    notifier = build_notifier(cfg)
    assert isinstance(notifier, EmailNotifier)
    assert notifier.smtp_port == 587
    assert notifier.recipients == ["team@example.com"]
