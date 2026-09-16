"""Apply pending PostgreSQL schema migrations without running the pipeline."""

from pathlib import Path
import argparse
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medalloan.config import Settings  # noqa: E402
from medalloan.database import create_db_engine, ensure_database_exists  # noqa: E402
from medalloan.migrations import apply_migrations  # noqa: E402
from medalloan.pipeline import (  # noqa: E402
    acquire_pipeline_lock,
    ensure_control_tables,
    release_pipeline_lock,
)


def main() -> int:
    argparse.ArgumentParser(description="Apply pending Medalloan database migrations").parse_args()
    settings = Settings.from_env()
    ensure_database_exists(settings)
    engine = create_db_engine(settings)
    lock = None
    try:
        lock = acquire_pipeline_lock(engine)
        ensure_control_tables(engine)
        applied = apply_migrations(engine)
        print("Applied migrations:", ", ".join(applied) if applied else "none")
        return 0
    finally:
        if lock is not None:
            release_pipeline_lock(lock)
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
