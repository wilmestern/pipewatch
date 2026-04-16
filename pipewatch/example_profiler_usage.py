"""Example demonstrating Profiler usage in pipewatch."""
import time
import random
from pipewatch.profiler import Profiler


def simulate_collection(source_name: str) -> float:
    """Simulate a metric collection run, returning elapsed seconds."""
    duration = random.uniform(0.1, 3.5)
    time.sleep(0)  # skip actual sleep in demo
    return duration


def run_demo() -> None:
    profiler = Profiler(slow_threshold_seconds=2.0)

    sources = ["postgres_pipeline", "kafka_consumer", "s3_ingestion", "api_poller"]

    print("=== Profiler Demo ===")
    for source in sources:
        duration = simulate_collection(source)
        entry = profiler.record(source, duration)
        slow_flag = " [SLOW]" if profiler.is_slow(source) else ""
        print(f"  Recorded: {entry.summary()}{slow_flag}")

    print()
    report = profiler.report()
    print(f"Total sources profiled : {report.total_sources}")
    print(f"Average duration       : {report.average_duration:.3f}s")
    if report.slowest:
        print(f"Slowest source         : {report.slowest.source_name} ({report.slowest.duration_seconds:.3f}s)")

    print()
    print("Report summary:")
    print(" ", report.summary())


if __name__ == "__main__":
    run_demo()
