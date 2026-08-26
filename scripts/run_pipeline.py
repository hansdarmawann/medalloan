from pathlib import Path
import argparse
import logging
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from retailion.config import ConfigurationError  # noqa: E402
from retailion.pipeline import PipelineError, run  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Retailion warehouse pipeline")
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "Sample - Superstore.csv")
    parser.add_argument("--start-date", help="Optional inclusive order date filter (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Optional inclusive order date filter (YYYY-MM-DD)")
    parser.add_argument(
        "--replay", action="store_true",
        help="Explicitly run a bounded replay/backfill window without advancing the watermark",
    )
    parser.add_argument(
        "--mode", choices=("full", "append", "upsert", "snapshot"), default="full",
        help="Bronze ingestion mode (default: full)",
    )
    parser.add_argument(
        "--overlap-days", type=int, default=2,
        help="Days to look back from the watermark for late-arriving data",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=None,
        help="Rows per extraction chunk; enables controlled batch ingestion",
    )
    parser.add_argument(
        "--throttle-ms", type=int, default=0,
        help="Delay between extraction chunks in milliseconds",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        run(args.source, start_date=args.start_date, end_date=args.end_date,
            replay=args.replay, load_mode=args.mode, overlap_days=args.overlap_days,
            chunk_size=args.chunk_size, throttle_ms=args.throttle_ms)
    except (ConfigurationError, PipelineError, FileNotFoundError, ValueError) as error:
        logging.error("Pipeline stopped: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
