"""Tests for pipewatch.router."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from pipewatch.alerts import Alert
from pipewatch.router import RouteRule, Router


def _alert(source: str = "db", name: str = "lag") -> Alert:
    return Alert(
        source_name=source,
        alert_name=name,
        message=f"{source}/{name} triggered",
        triggered_at=datetime(2024, 1, 1, 12, 0, 0),
    )


@pytest.fixture()
def router() -> Router:
    return Router(rules=[])


# --- RouteRule.matches ---

def test_rule_matches_any_when_no_filters():
    rule = RouteRule(backend_names=["log"])
    assert rule.matches(_alert("db", "lag")) is True


def test_rule_matches_specific_source():
    rule = RouteRule(backend_names=["email"], source_name="db")
    assert rule.matches(_alert("db", "lag")) is True
    assert rule.matches(_alert("api", "lag")) is False


def test_rule_matches_specific_alert_name():
    rule = RouteRule(backend_names=["email"], alert_name="lag")
    assert rule.matches(_alert("db", "lag")) is True
    assert rule.matches(_alert("db", "error_rate")) is False


def test_rule_matches_both_source_and_name():
    rule = RouteRule(backend_names=["pager"], source_name="db", alert_name="lag")
    assert rule.matches(_alert("db", "lag")) is True
    assert rule.matches(_alert("db", "error_rate")) is False
    assert rule.matches(_alert("api", "lag")) is False


# --- Router.register ---

def test_register_adds_backend(router):
    backend = MagicMock()
    router.register("log", backend)
    assert "log" in router.backends


def test_register_overwrites_existing_backend(router):
    """Registering a backend under an existing name replaces the previous one."""
    backend_a = MagicMock()
    backend_b = MagicMock()
    router.register("log", backend_a)
    router.register("log", backend_b)
    assert router.backends["log"] is backend_b


# --- Router.route ---

def test_route_sends_to_matching_backend(router):
    backend = MagicMock()
    backend.send.return_value = True
    router.register("log", backend)
    router.rules.append(RouteRule(backend_names=["log"], source_name="db"))

    alerts = [_alert("db", "lag")]
    sent = router.route(alerts)

    backend.send.assert_called_once_with(alerts)
    assert sent["log"] == alerts


def test_route_skips_non_matching_rule(router):
    backend = MagicMock()
    router.register("email", backend)
    router.rules.append(RouteRule(backend_names=["email"], source_name="api"))

    sent = router.route([_alert("db", "lag")])

    backend.send.assert_not_called()
    assert sent["email"] == []


def test_route_uses_default_backend_when_no_rule_matches(router):
    backend = MagicMock()
    router.register("log", backend)
    router.default_backend = "log"

    alerts = [_alert("unknown", "mystery")]
    sent = router.route(alerts)

    backend.send.assert_called_once_with(alerts)
    assert sent["log"] == alerts


def test_route_returns_empty_when_no_rules_and_no_default(router):
    backend = MagicMock()
    router.register("log", backend)

    sent = router.route([_alert()])

    backend.send.assert_not_called()
    assert sent["log"] == []


def test_route_first_matching_rule_wins(router):
    backend_a = MagicMock()
    backend_b = MagicMock
