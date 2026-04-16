"""Example demonstrating QuotaManager usage in pipewatch."""
from pipewatch.quota import QuotaRule, QuotaManager


def run_demo() -> None:
    rules = [
        QuotaRule(source_name="db_pipeline", max_runs=3, window_seconds=60),
        QuotaRule(source_name=None, max_runs=5, window_seconds=60),
    ]
    manager = QuotaManager(rules=rules)

    sources = ["db_pipeline", "api_pipeline", "db_pipeline",
               "db_pipeline", "db_pipeline", "api_pipeline"]

    for source in sources:
        result = manager.check_and_record(source)
        print(result.summary())


if __name__ == "__main__":
    run_demo()
