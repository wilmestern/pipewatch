"""Example demonstrating HistoryStore integration with the scheduler loop.

This script is not part of the production runtime; it exists to illustrate
how history tracking fits into the broader pipewatch architecture.
"""

import time
import logging

from pipewatch.config import load_config
from pipewatch.metrics import MetricsCollector
from pipewatch.alerts import AlertEvaluator
from pipewatch.history import HistoryStore
from pipewatch.notifier import LogNotifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CONFIG_PATH = "pipewatch/example_config.yaml"
CONSECUTIVE_FAILURE_THRESHOLD = 3


def run_demo(iterations: int = 5, interval: float = 2.0) -> None:
    config = load_config(CONFIG_PATH)
    store = HistoryStore(max_entries=50)
    notifier = LogNotifier()

    for source_cfg in config.sources:
        collector = MetricsCollector(source_cfg)
        evaluator = AlertEvaluator(source_cfg)

        for i in range(iterations):
            log.info("[%s] Poll %d/%d", source_cfg.name, i + 1, iterations)
            result = collector.collect()
            store.record(source_cfg.name, result)

            alerts = evaluator.evaluate(result.metric)
            if alerts:
                notifier.send(alerts)

            history = store.get(source_cfg.name)
            consec = history.consecutive_failures()
            rate = history.failure_rate()
            log.info(
                "[%s] failure_rate=%.0f%% consecutive_failures=%d",
                source_cfg.name,
                rate * 100,
                consec,
            )

            if consec >= CONSECUTIVE_FAILURE_THRESHOLD:
                log.warning(
                    "[%s] %d consecutive failures — consider paging on-call!",
                    source_cfg.name,
                    consec,
                )

            if i < iterations - 1:
                time.sleep(interval)

    log.info("Demo complete.")


if __name__ == "__main__":
    run_demo()
