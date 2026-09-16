"""Run the Medalloan DAG from a manual, scheduled, or event trigger."""

import argparse
import logging
import sys
from itertools import count
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medalloan.orchestrator import DAG, Task  # noqa: E402
from medalloan.pipeline import run  # noqa: E402


def build_dag(args) -> DAG:
    dag = DAG(
        name="medalloan_loan_applications",
        schedule=args.schedule,
        event_triggers={"source_updated", "backfill_requested"},
        max_concurrency=1,
        resource_pools={"postgres": 1},
    )
    # The pipeline task owns database transaction boundaries; the preceding
    # control-plane tasks make the dependency contract explicit.
    dag.add_task(Task("source_ready", action=lambda: True,
                      max_attempts=2, backoff_seconds=1, timeout_seconds=30,
                      resource_pool="postgres"))
    attempts_seen = count()

    def load_transform_publish():
        attempt = next(attempts_seen) + 1
        if args.inject_failure and attempt == 1:
            raise RuntimeError("Injected demo failure; retry should recover")
        return run(args.source, start_date=args.start_date,
                   end_date=args.end_date, replay=args.replay,
                   load_mode=args.mode, overlap_days=args.overlap_days,
                   chunk_size=args.chunk_size, throttle_ms=args.throttle_ms)

    dag.add_task(Task("load_transform_publish", upstream=("source_ready",),
                      action=load_transform_publish,
                      max_attempts=3, backoff_seconds=2, timeout_seconds=3600,
                      resource_pool="postgres"))
    dag.add_task(Task("publish_audit", upstream=("load_transform_publish",),
                      action=lambda: True, timeout_seconds=30,
                      resource_pool="postgres"))
    return dag


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Medalloan orchestration DAG")
    parser.add_argument("--source", type=Path, default=ROOT / "data",
                        help="A loan CSV file or directory containing loan_data_*.csv files")
    parser.add_argument("--mode", choices=("full", "append", "upsert", "snapshot"), default="full")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--overlap-days", type=int, default=2)
    parser.add_argument("--chunk-size", type=int)
    parser.add_argument("--throttle-ms", type=int, default=0)
    parser.add_argument(
        "--inject-failure", action="store_true",
        help="Demo recovery: fail the first pipeline task attempt, then retry successfully",
    )
    parser.add_argument("--trigger", default="manual",
                        help="manual, source_updated, backfill_requested, or the schedule value")
    parser.add_argument("--schedule", default="0 2 * * *",
                        help="cron metadata for external scheduler integration")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_dag(args).run(args.trigger)
    logging.info("DAG completed: medalloan_loan_applications")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
