"""Example demonstrating Tracer usage in a simulated pipeline run."""
import time
from pipewatch.tracer import Tracer


def simulate_run(tracer: Tracer, source_name: str, fail: bool = False) -> None:
    span = tracer.start(source_name)
    time.sleep(0.05)
    if fail:
        tracer.finish(span, success=False, error="simulated failure")
    else:
        tracer.finish(span, success=True)
    print(span.summary())


def run_demo() -> None:
    tracer = Tracer()

    sources = [
        ("db_pipeline", False),
        ("api_pipeline", False),
        ("file_pipeline", True),
        ("db_pipeline", False),
    ]

    for name, fail in sources:
        simulate_run(tracer, name, fail=fail)

    print("\n--- Latest spans ---")
    for source in tracer.all_sources():
        latest = tracer.latest(source)
        if latest:
            print(f"  {source}: {latest.summary()}")

    print("\n--- All db_pipeline spans ---")
    for span in tracer.spans_for("db_pipeline"):
        print(f"  {span.summary()}")


if __name__ == "__main__":
    run_demo()
