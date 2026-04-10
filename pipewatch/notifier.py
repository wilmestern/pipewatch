"""Notification backends for pipewatch alerts."""

from __future__ import annotations

import logging
import smtplib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from email.message import EmailMessage
from typing import List

from pipewatch.alerts import Alert

logger = logging.getLogger(__name__)


class NotificationBackend(ABC):
    """Abstract base for notification backends."""

    @abstractmethod
    def send(self, alerts: List[Alert]) -> bool:
        """Send notifications for the given alerts. Returns True on success."""


@dataclass
class LogNotifier(NotificationBackend):
    """Logs alerts using the standard logging module (default / fallback)."""

    level: str = "WARNING"

    def send(self, alerts: List[Alert]) -> bool:
        log_fn = getattr(logger, self.level.lower(), logger.warning)
        for alert in alerts:
            log_fn(
                "[pipewatch] ALERT source=%s metric=%s value=%s threshold=%s",
                alert.source_name,
                alert.metric_name,
                alert.current_value,
                alert.threshold,
            )
        return True


@dataclass
class EmailNotifier(NotificationBackend):
    """Sends alert summaries via SMTP email."""

    smtp_host: str
    smtp_port: int
    sender: str
    recipients: List[str]
    subject_prefix: str = "[pipewatch]"

    def send(self, alerts: List[Alert]) -> bool:
        if not alerts:
            return True
        body_lines = ["The following pipeline alerts are active:\n"]
        for alert in alerts:
            body_lines.append(
                f"  - {alert.source_name}/{alert.metric_name}: "
                f"value={alert.current_value}, threshold={alert.threshold}"
            )
        msg = EmailMessage()
        msg["Subject"] = f"{self.subject_prefix} {len(alerts)} alert(s) detected"
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg.set_content("\n".join(body_lines))
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=10) as server:
                server.send_message(msg)
            logger.info("Email notification sent to %s", self.recipients)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to send email notification: %s", exc)
            return False


def build_notifier(config: dict) -> NotificationBackend:
    """Factory that builds a notifier from a config dict."""
    backend = config.get("backend", "log")
    if backend == "email":
        return EmailNotifier(
            smtp_host=config["smtp_host"],
            smtp_port=int(config.get("smtp_port", 25)),
            sender=config["sender"],
            recipients=config["recipients"],
            subject_prefix=config.get("subject_prefix", "[pipewatch]"),
        )
    return LogNotifier(level=config.get("level", "WARNING"))
