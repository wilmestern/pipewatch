"""Example: using Reaper to clean up inactive sources from CheckpointStore."""

from datetime import datetime, timedelta

from pipewatch.checkpoint import CheckpointStore
from pipewatch.reaper import Reaper, ReaperPolicy


def run_demo() -> None:
    store = CheckpointStore()
    now = datetime.utcnow()

    # Simulate sources with varying last-seen times
    store.update("etl-daily", now - timedelta(seconds=60))       # active
    store.update("etl-weekly", now - timedelta(seconds=7200))    # stale (2h)
    store.update("legacy-feed", now - timedelta(seconds=86400))  # very stale (1d)

    policy = ReaperPolicy(max_inactivity_seconds=3600, dry_run=False)
    reaper = Reaper(policy=policy, store=store)

    print("=== Reaper Demo ===")
    print(f"Sources before reaping: {list(store.all_sources())}")

    results = reaper.run(now=now)
    print(f"\nReaped {len(results)} source(s):")
    for r in results:
        print(f"  {r.summary()}")

    print(f"\nSources after reaping: {list(store.all_sources())}")

    # Dry-run demo
    store.update("orphan-job", now - timedelta(seconds=9000))
    dry_policy = ReaperPolicy(max_inactivity_seconds=3600, dry_run=True)
    dry_reaper = Reaper(policy=dry_policy, store=store)
    dry_results = dry_reaper.run(now=now)
    print("\n=== Dry Run ===")
    for r in dry_results:
        print(f"  {r.summary()}")
    print(f"Sources still present after dry run: {list(store.all_sources())}")


if __name__ == "__main__":
    run_demo()
