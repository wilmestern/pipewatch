"""Scheduler for periodic pipeline metric collection and alerting."""

import logging
import time
from typing import Callable, Optional

from pipewatch.config import PipewatchConfig
from pipewatch.metrics import MetricsCollector
from pipewatch.alerts import AlertEvaluator
from pipewatch.reporter import Reporter

logger = logging.getLogger(__name__)


class Scheduler:
    """Runs the collect-evaluate-report loop on a configurable interval."""

    def __init__(
        self,
        config: PipewatchConfig,
        collector: MetricsCollector,
        evaluator: AlertEvaluator,
        reporter: Reporter,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.config = config
        self.collector = collector
        self.evaluator = evaluator
        self.reporter = reporter
        self._sleep = sleep_fn or time.sleep
        self._running = False

    def run_once(self) -> None:
        """Perform a single collect → evaluate → report cycle."""
        logger.debug("Starting collection cycle.")
        for source in self.config.sources:
            result = self.collector.collect(source)
            if result is None:
                logger.warning("No metric returned for source '%s'.", source.name)
                continue
            self.evaluator.evaluate(result)
            report = self.reporter.build_report(source, result, self.evaluator)
            self.reporter.emit(report)
        logger.debug("Collection cycle complete.")

    def start(self) -> None:
        """Run the scheduler loop until stopped."""
        self._running = True
        logger.info(
            "Scheduler started (poll_interval=%ss).", self.config.poll_interval
        )
        try:
            while self._running:
                self.run_once()
                self._sleep(self.config.poll_interval)
        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user.")
        finally:
            self._running = False
            logger.info("Scheduler stopped.")

    def stop(self) -> None:
        """Signal the scheduler loop to stop after the current cycle."""
        self._running = False
