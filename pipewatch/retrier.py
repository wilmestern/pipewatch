"""Retry logic for transient metric collection failures."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Configuration for retry behaviour."""

    max_attempts: int = 3
    delay_seconds: float = 1.0
    backoff_factor: float = 2.0
    max_delay_seconds: float = 30.0

    def delay_for(self, attempt: int) -> float:
        """Return the delay in seconds before *attempt* (0-indexed)."""
        if attempt == 0:
            return 0.0
        raw = self.delay_seconds * (self.backoff_factor ** (attempt - 1))
        return min(raw, self.max_delay_seconds)


@dataclass
class RetryResult:
    """Outcome of a retried operation."""

    success: bool
    attempts: int
    value: object = None
    last_exception: Optional[Exception] = None

    @property
    def summary(self) -> str:
        if self.success:
            return f"succeeded after {self.attempts} attempt(s)"
        return (
            f"failed after {self.attempts} attempt(s): {self.last_exception}"
        )


class Retrier:
    """Executes a callable with retry logic defined by a RetryPolicy."""

    def __init__(self, policy: Optional[RetryPolicy] = None) -> None:
        self._policy = policy or RetryPolicy()

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    def run(self, fn: Callable[[], object], source_name: str = "") -> RetryResult:
        """Call *fn* up to policy.max_attempts times, returning a RetryResult."""
        last_exc: Optional[Exception] = None

        for attempt in range(self._policy.max_attempts):
            delay = self._policy.delay_for(attempt)
            if delay > 0:
                logger.debug(
                    "Retrier: waiting %.1fs before attempt %d for '%s'",
                    delay,
                    attempt + 1,
                    source_name,
                )
                time.sleep(delay)

            try:
                value = fn()
                logger.debug(
                    "Retrier: '%s' succeeded on attempt %d",
                    source_name,
                    attempt + 1,
                )
                return RetryResult(success=True, attempts=attempt + 1, value=value)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Retrier: attempt %d for '%s' raised %s",
                    attempt + 1,
                    source_name,
                    exc,
                )

        return RetryResult(
            success=False,
            attempts=self._policy.max_attempts,
            last_exception=last_exc,
        )
