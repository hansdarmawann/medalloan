"""Small, ordered PostgreSQL migration runner for the warehouse schema."""

from __future__ import annotations

import hashlib
from pathlib import Path

from sqlalchemy import text



class MigrationError(RuntimeError):
    """Raised when migrations cannot be applied safely."""


MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"


def _migration_files() -> list[Path]:
    if not MIGRATIONS_DIR.is_dir():
        raise MigrationError(f"Migration directory not found: {MIGRATIONS_DIR}")
    return sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))


def apply_migrations(engine) -> list[str]:
    """Apply pending SQL migrations and reject changed applied migrations."""
    applied: list[str] = []
    with engine.begin() as connection:
        ledger_exists = connection.execute(text(
            "SELECT to_regclass('control.schema_migrations') IS NOT NULL"
        )).scalar_one()
        for migration_file in _migration_files():
            version, _, description = migration_file.stem.partition("_")
            sql_text = migration_file.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()
            existing = None
            if ledger_exists:
                existing = connection.execute(text("""
                    SELECT checksum FROM control.schema_migrations WHERE version = :version
                """), {"version": version}).scalar_one_or_none()
            elif version != "001":
                raise MigrationError(
                    "Migration ledger is missing; apply 001_control_schema_migrations.sql first"
                )
            if existing is not None:
                if existing != checksum:
                    raise MigrationError(
                        f"Applied migration {version} has changed; create a new migration instead"
                    )
                continue

            connection.exec_driver_sql(sql_text)
            connection.execute(text("""
                INSERT INTO control.schema_migrations (version, description, checksum)
                VALUES (:version, :description, :checksum)
            """), {"version": version, "description": description, "checksum": checksum})
            ledger_exists = True
            applied.append(version)
    return applied
