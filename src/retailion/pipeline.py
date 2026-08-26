from pathlib import Path
import hashlib
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text

from .config import ConfigurationError, Settings
from .database import create_db_engine

LOGGER = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Base class for expected pipeline failures."""


class DataQualityError(PipelineError):
    """Raised when stop-mode data quality checks fail."""
QUALITY_FAILURE_MODE = os.getenv("QUALITY_FAILURE_MODE", "STOP").upper()
QUALITY_RULE_VERSION = os.getenv("QUALITY_RULE_VERSION", "1.0.0")
PIPELINE_SLA_SECONDS = int(os.getenv("PIPELINE_SLA_SECONDS", "3600"))
REQUIRED_SOURCE_COLUMNS = {
    "Row ID", "Order ID", "Order Date", "Ship Date", "Customer ID",
    "Product ID", "Sales", "Quantity", "Discount", "Profit",
}


def validate_source_schema(columns) -> None:
    """Fail fast when the source contract changes unexpectedly."""
    missing = REQUIRED_SOURCE_COLUMNS.difference(columns)
    if missing:
        raise ValueError(
            "Source schema validation failed; missing columns: "
            + ", ".join(sorted(missing))
        )


def ensure_watermark_table(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS control;
            CREATE TABLE IF NOT EXISTS control.pipeline_watermarks (
                pipeline_name TEXT PRIMARY KEY,
                watermark_date TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        # Migrate installations created by earlier versions.  The source only
        # exposes a date, so historical values are interpreted as UTC midnight.
        connection.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'control'
                      AND table_name = 'pipeline_watermarks'
                      AND column_name = 'watermark_date'
                      AND data_type = 'date'
                ) THEN
                    ALTER TABLE control.pipeline_watermarks
                    ALTER COLUMN watermark_date TYPE TIMESTAMPTZ
                    USING watermark_date::TIMESTAMP AT TIME ZONE 'UTC';
                END IF;
            END $$;
        """))


def read_watermark(engine, pipeline_name: str):
    with engine.connect() as connection:
        return connection.execute(text("""
            SELECT watermark_date
            FROM control.pipeline_watermarks
            WHERE pipeline_name = :pipeline_name
        """), {"pipeline_name": pipeline_name}).scalar_one_or_none()


def advance_watermark(engine, pipeline_name: str, watermark_date) -> None:
    if watermark_date is None:
        return
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO control.pipeline_watermarks
                (pipeline_name, watermark_date)
            VALUES (:pipeline_name, :watermark_date)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                watermark_date = GREATEST(
                    control.pipeline_watermarks.watermark_date,
                    EXCLUDED.watermark_date
                ),
                updated_at = CURRENT_TIMESTAMP
        """), {"pipeline_name": pipeline_name, "watermark_date": watermark_date})


def new_run_id() -> str:
    """Return an RFC 9562 version-7 UUID without external dependencies."""
    timestamp_ms = time.time_ns() // 1_000_000
    random_bits = secrets.randbits(76)
    value = (timestamp_ms << 80) | (0x7 << 76) | random_bits
    value = (value & ~(0b11 << 62)) | (0b10 << 62)
    return str(uuid.UUID(int=value))


def run_bronze(engine, source_path: Path, load_mode: str = "full", run_id: str | None = None,
               chunk_size: int | None = None, throttle_ms: int = 0) -> int:
    if load_mode not in {"full", "append", "upsert", "snapshot"}:
        raise ValueError(f"Unsupported load mode: {load_mode}")
    LOGGER.info("Loading %s into bronze.superstore", source_path)
    fingerprint_before = hashlib.sha256(source_path.read_bytes()).hexdigest()
    if chunk_size and chunk_size > 0:
        chunks = []
        for chunk in pd.read_csv(source_path, encoding="latin-1", chunksize=chunk_size):
            chunks.append(chunk)
            if throttle_ms > 0:
                time.sleep(throttle_ms / 1000)
        frame = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    else:
        frame = pd.read_csv(source_path, encoding="latin-1")
    fingerprint_after = hashlib.sha256(source_path.read_bytes()).hexdigest()
    if fingerprint_before != fingerprint_after:
        raise RuntimeError("Source changed during extraction; load aborted for consistency")
    validate_source_schema(frame.columns)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS control"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.source_schema_registry (
                source_name TEXT PRIMARY KEY,
                schema_hash TEXT NOT NULL,
                columns TEXT NOT NULL,
                schema_version INTEGER NOT NULL DEFAULT 1,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.source_schema_changes (
                source_name TEXT NOT NULL,
                run_id UUID NOT NULL,
                previous_hash TEXT,
                new_hash TEXT NOT NULL,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_name, run_id)
            )
        """))
        connection.execute(text("""
            ALTER TABLE control.source_schema_registry
            ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1
        """))
        schema_columns = ",".join(str(column) for column in frame.columns)
        schema_hash = hashlib.sha256(schema_columns.encode("utf-8")).hexdigest()
        previous_hash = connection.execute(text("""
            SELECT schema_hash FROM control.source_schema_registry
            WHERE source_name = :source_name
        """), {"source_name": source_path.name}).scalar_one_or_none()
        if previous_hash and previous_hash != schema_hash:
            connection.execute(text("""
                INSERT INTO control.source_schema_changes
                    (source_name, run_id, previous_hash, new_hash)
                VALUES (:source_name, :run_id, :previous_hash, :new_hash)
                ON CONFLICT DO NOTHING
            """), {"source_name": source_path.name, "run_id": run_id,
                    "previous_hash": previous_hash, "new_hash": schema_hash})
        connection.execute(text("""
            INSERT INTO control.source_schema_registry
                (source_name, schema_hash, columns, schema_version)
            VALUES (:source_name, :schema_hash, :columns,
                    COALESCE((SELECT schema_version + 1
                              FROM control.source_schema_registry
                              WHERE source_name = :source_name), 1))
            ON CONFLICT (source_name) DO UPDATE SET
                schema_hash = EXCLUDED.schema_hash,
                columns = EXCLUDED.columns,
                observed_at = CURRENT_TIMESTAMP
        """), {"source_name": source_path.name, "schema_hash": schema_hash,
                "columns": schema_columns})
    if load_mode == "snapshot":
        snapshot = frame.copy()
        snapshot["snapshot_run_id"] = run_id
        snapshot["snapshot_at"] = datetime.now(timezone.utc)
        snapshot.to_sql("superstore_snapshots", engine, schema="bronze", if_exists="append", index=False)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        connection.execute(text("DROP TABLE IF EXISTS bronze.superstore_stage"))
    frame.to_sql("superstore_stage", engine, schema="bronze", if_exists="replace", index=False)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.superstore
            (LIKE bronze.superstore_stage INCLUDING DEFAULTS);
        """))
        # Make the source business key a database-enforced idempotency boundary.
        # Clean legacy duplicates before adding the constraint so upgrades are
        # safe for databases created by earlier versions.
        connection.execute(text("""
            DELETE FROM bronze.superstore duplicate
            USING bronze.superstore keeper
            WHERE duplicate.ctid < keeper.ctid
              AND duplicate."Row ID" = keeper."Row ID";
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'bronze.superstore'::regclass
                      AND contype = 'u'
                      AND conname = 'uq_bronze_superstore_row_id'
                ) THEN
                    ALTER TABLE bronze.superstore
                        ADD CONSTRAINT uq_bronze_superstore_row_id UNIQUE ("Row ID");
                END IF;
            END $$;
        """))
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS control;
            CREATE TABLE IF NOT EXISTS control.source_deletions (
                run_id UUID NOT NULL,
                row_id TEXT NOT NULL,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, row_id)
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.cdc_events (
                run_id UUID NOT NULL,
                row_id TEXT NOT NULL,
                operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
                detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, row_id, operation)
            )
        """))
        if run_id is not None:
            params = {"run_id": run_id}
            connection.execute(text("""
                INSERT INTO control.cdc_events (run_id, row_id, operation)
                SELECT :run_id, CAST(incoming."Row ID" AS TEXT), 'INSERT'
                FROM bronze.superstore_stage incoming
                WHERE NOT EXISTS (
                    SELECT 1 FROM bronze.superstore existing
                    WHERE existing."Row ID" = incoming."Row ID"
                )
                ON CONFLICT DO NOTHING
            """), params)
            connection.execute(text("""
                INSERT INTO control.cdc_events (run_id, row_id, operation)
                SELECT :run_id, CAST(incoming."Row ID" AS TEXT), 'UPDATE'
                FROM bronze.superstore_stage incoming
                JOIN bronze.superstore existing
                  ON existing."Row ID" = incoming."Row ID"
                WHERE md5(row_to_json(existing)::text) <> md5(row_to_json(incoming)::text)
                ON CONFLICT DO NOTHING
            """), params)
            connection.execute(text("""
                INSERT INTO control.cdc_events (run_id, row_id, operation)
                SELECT :run_id, CAST(existing."Row ID" AS TEXT), 'DELETE'
                FROM bronze.superstore existing
                WHERE NOT EXISTS (
                    SELECT 1 FROM bronze.superstore_stage incoming
                    WHERE incoming."Row ID" = existing."Row ID"
                )
                ON CONFLICT DO NOTHING
            """), params)
        if load_mode in {"full", "upsert", "snapshot"} and run_id is not None:
            connection.execute(text("""
                INSERT INTO control.source_deletions (run_id, row_id)
                SELECT :run_id, CAST(existing."Row ID" AS TEXT)
                FROM bronze.superstore existing
                WHERE NOT EXISTS (
                    SELECT 1 FROM bronze.superstore_stage incoming
                    WHERE incoming."Row ID" = existing."Row ID"
                )
                ON CONFLICT DO NOTHING
            """), {"run_id": run_id})
        if load_mode == "full" or load_mode == "snapshot":
            connection.execute(text("TRUNCATE TABLE bronze.superstore"))
        elif load_mode in {"append", "upsert"}:
            connection.execute(text("""
                DELETE FROM bronze.superstore target
                USING bronze.superstore_stage stage
                WHERE target."Row ID" = stage."Row ID"
            """))
        connection.execute(text("""
            INSERT INTO bronze.superstore
            SELECT DISTINCT ON ("Row ID") *
            FROM bronze.superstore_stage
            ORDER BY "Row ID";
            DROP TABLE bronze.superstore_stage;
            COMMENT ON TABLE bronze.superstore IS
                'Raw source snapshot loaded through a staging table.';
        """))
    return len(frame)


def run_silver(engine, run_id: str, source_file: str, start_date=None, end_date=None) -> int:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS control;
            CREATE TABLE IF NOT EXISTS control.data_profile_results (
                run_id UUID NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value NUMERIC NOT NULL,
                profiled_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, metric_name)
            )
        """))
        profile_rows = connection.execute(text("""
            SELECT COUNT(*) AS total_rows,
                   COUNT(*) FILTER (WHERE "Order ID" IS NULL) AS null_order_ids,
                   COUNT(*) - COUNT(DISTINCT "Row ID") AS duplicate_row_ids
            FROM bronze.superstore
        """)).mappings().one()
        connection.execute(text("""
            INSERT INTO control.data_profile_results
                (run_id, metric_name, metric_value)
            VALUES (:run_id, :total_rows, :total_value),
                   (:run_id, :null_order_ids, :null_value),
                   (:run_id, :duplicate_row_ids, :duplicate_value)
            ON CONFLICT (run_id, metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                profiled_at = CURRENT_TIMESTAMP
        """), {
            "run_id": run_id,
            "total_rows": "total_rows",
            "total_value": profile_rows["total_rows"],
            "null_order_ids": "null_order_ids",
            "null_value": profile_rows["null_order_ids"],
            "duplicate_row_ids": "duplicate_row_ids",
            "duplicate_value": profile_rows["duplicate_row_ids"],
        })
    sql = """
    CREATE SCHEMA IF NOT EXISTS silver;
    DROP TABLE IF EXISTS silver.superstore;
    CREATE TABLE silver.superstore AS
    SELECT DISTINCT ON ("Row ID")
        CAST("Row ID" AS INTEGER) AS row_id,
        "Order ID" AS order_id,
        TO_DATE("Order Date", 'MM/DD/YYYY') AS order_date,
        TO_DATE("Ship Date", 'MM/DD/YYYY') AS ship_date,
        "Ship Mode" AS ship_mode,
        "Customer ID" AS customer_id,
        TRIM("Customer Name") AS customer_name,
        "Segment" AS segment,
        "Country" AS country,
        "City" AS city,
        "State" AS state,
        COALESCE(CAST("Postal Code" AS VARCHAR), '00000') AS postal_code,
        "Region" AS region,
        "Product ID" AS product_id,
        "Category" AS category,
        "Sub-Category" AS sub_category,
        "Product Name" AS product_name,
        CAST("Sales" AS DOUBLE PRECISION) AS sales,
        CAST("Quantity" AS INTEGER) AS quantity,
        CAST("Discount" AS DOUBLE PRECISION) AS discount,
        CAST("Profit" AS DOUBLE PRECISION) AS profit,
        CURRENT_TIMESTAMP AS ingested_at,
        :run_id AS run_id,
        :source_file AS source_file,
        '1.0.0' AS pipeline_version
    FROM bronze.superstore
    WHERE "Order ID" IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM control.source_deletions deleted
          WHERE deleted.run_id = :run_id
            AND deleted.row_id = CAST("Row ID" AS TEXT)
      )
      AND (:start_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') >= CAST(:start_date AS DATE))
      AND (:end_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') <= CAST(:end_date AS DATE))
    ORDER BY "Row ID";
    COMMENT ON TABLE silver.superstore IS
        'Grain: one row per source transaction row_id.';
    """
    with engine.begin() as connection:
        connection.execute(text(sql), {"run_id": run_id, "source_file": source_file,
                                       "start_date": start_date, "end_date": end_date})
        connection.execute(text("ALTER TABLE silver.superstore ADD PRIMARY KEY (row_id)"))
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS quarantine;
            CREATE TABLE IF NOT EXISTS quarantine.superstore_invalid AS
            SELECT *, CAST(NULL AS TEXT) AS reason
            FROM silver.superstore WITH NO DATA;
            ALTER TABLE quarantine.superstore_invalid
                ADD COLUMN IF NOT EXISTS reason TEXT;
        """))
        connection.execute(text(
            "DELETE FROM quarantine.superstore_invalid WHERE run_id = :run_id"
        ), {"run_id": run_id})
        connection.execute(text("""
            INSERT INTO quarantine.superstore_invalid
            SELECT s.*,
                   CASE WHEN quantity <= 0 THEN 'quantity <= 0'
                        WHEN discount NOT BETWEEN 0 AND 1 THEN 'discount outside [0,1]'
                        WHEN sales < 0 THEN 'sales < 0' END
            FROM silver.superstore s
            WHERE quantity <= 0 OR discount NOT BETWEEN 0 AND 1 OR sales < 0
        """), {"run_id": run_id})
        silver_count = connection.execute(text("SELECT COUNT(*) FROM silver.superstore")).scalar_one()
        bronze_count = connection.execute(text("""
            SELECT COUNT(*)
            FROM bronze.superstore
            WHERE "Order ID" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM control.source_deletions deleted
                  WHERE deleted.run_id = :run_id
                    AND deleted.row_id = CAST("Row ID" AS TEXT)
              )
              AND (:start_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') >= CAST(:start_date AS DATE))
              AND (:end_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') <= CAST(:end_date AS DATE))
        """), {"run_id": run_id, "start_date": start_date, "end_date": end_date}).scalar_one()
        if silver_count != bronze_count:
            raise RuntimeError(
                f"Bronze/Silver reconciliation failed: bronze={bronze_count}, silver={silver_count}"
            )
        return silver_count


def count_source_rows(engine, run_id: str, start_date=None, end_date=None) -> int:
    """Count source rows in the requested window before replacing Silver."""
    with engine.connect() as connection:
        return connection.execute(text("""
            SELECT COUNT(*)
            FROM bronze.superstore
            WHERE "Order ID" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM control.source_deletions deleted
                  WHERE deleted.run_id = :run_id
                    AND deleted.row_id = CAST("Row ID" AS TEXT)
              )
              AND (:start_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') >= CAST(:start_date AS DATE))
              AND (:end_date IS NULL OR TO_DATE("Order Date", 'MM/DD/YYYY') <= CAST(:end_date AS DATE))
        """), {"run_id": run_id, "start_date": start_date, "end_date": end_date}).scalar_one()


def mark_pipeline_run(engine, run_id: str, status: str, bronze_rows: int,
                       silver_rows: int, gold_rows: int) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            UPDATE control.pipeline_runs
            SET finished_at = :finished_at, status = :status,
                bronze_rows = :bronze_rows, silver_rows = :silver_rows,
                gold_rows = :gold_rows
            WHERE run_id = :run_id
        """), {
            "finished_at": datetime.now(timezone.utc),
            "status": status,
            "bronze_rows": bronze_rows,
            "silver_rows": silver_rows,
            "gold_rows": gold_rows,
            "run_id": run_id,
        })


def ensure_observability_tables(engine) -> None:
    """Create queryable metrics, trace, and lineage metadata tables."""
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS control;
            CREATE TABLE IF NOT EXISTS control.pipeline_metrics (
                run_id UUID NOT NULL, metric_name TEXT NOT NULL,
                metric_value NUMERIC NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, metric_name)
            );
            CREATE TABLE IF NOT EXISTS control.pipeline_traces (
                trace_id UUID NOT NULL, span_id UUID PRIMARY KEY, run_id UUID NOT NULL,
                stage TEXT NOT NULL, status TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ,
                error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS control.data_lineage (
                run_id UUID NOT NULL, source_object TEXT NOT NULL,
                target_object TEXT NOT NULL, relation TEXT NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, source_object, target_object)
            );
            CREATE TABLE IF NOT EXISTS control.incident_evidence (
                run_id UUID PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                failure_stage TEXT NOT NULL,
                failed_at TIMESTAMPTZ NOT NULL,
                error_message TEXT NOT NULL,
                impact TEXT NOT NULL,
                owner TEXT NOT NULL,
                safe_next_action TEXT NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """))


def publish_observability(engine, run_id: str, bronze_rows: int,
                          silver_rows: int, gold_rows: int,
                          started_at: datetime, finished_at: datetime) -> None:
    """Publish metrics, a correlated trace span, and layer lineage."""
    with engine.begin() as connection:
        params = {"run_id": run_id, "bronze_rows": bronze_rows,
                  "silver_rows": silver_rows, "gold_rows": gold_rows,
                  "duration": (finished_at - started_at).total_seconds()}
        connection.execute(text("""
            INSERT INTO control.pipeline_metrics (run_id, metric_name, metric_value)
            VALUES (:run_id, 'bronze_rows', :bronze_rows),
                   (:run_id, 'silver_rows', :silver_rows),
                   (:run_id, 'gold_rows', :gold_rows),
                   (:run_id, 'duration_seconds', :duration)
            ON CONFLICT (run_id, metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value, recorded_at = CURRENT_TIMESTAMP
        """), params)
        connection.execute(text("""
            INSERT INTO control.pipeline_traces
                (trace_id, span_id, run_id, stage, status, started_at, finished_at)
            VALUES (:run_id, :span_id, :run_id, 'pipeline', 'SUCCESS', :started_at, :finished_at)
        """), {"run_id": run_id, "span_id": uuid.uuid4(),
                "started_at": started_at, "finished_at": finished_at})
        connection.execute(text("""
            INSERT INTO control.data_lineage
                (run_id, source_object, target_object, relation)
            VALUES (:run_id, 'source:Sample - Superstore.csv', 'bronze.superstore', 'ingests'),
                   (:run_id, 'bronze.superstore', 'silver.superstore', 'transforms'),
                   (:run_id, 'silver.superstore', 'gold.fact_sales', 'serves')
            ON CONFLICT DO NOTHING
        """), {"run_id": run_id})


def apply_security_governance(engine, retention_days: int = 90) -> None:
    """Publish classification, masking, encryption, and retention controls."""
    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.data_classification (
                object_name TEXT NOT NULL, column_name TEXT NOT NULL,
                classification TEXT NOT NULL,
                encryption_required BOOLEAN NOT NULL DEFAULT FALSE,
                masking_policy TEXT,
                retention_days INTEGER NOT NULL,
                PRIMARY KEY (object_name, column_name)
            );
            INSERT INTO control.data_classification
                (object_name, column_name, classification, encryption_required,
                 masking_policy, retention_days)
            VALUES
                ('gold.dim_customers', 'customer_id', 'CONFIDENTIAL', TRUE, 'hash', :days),
                ('gold.dim_customers', 'customer_name', 'PERSONAL', FALSE, 'partial', :days),
                ('control.pipeline_runs', 'error_message', 'INTERNAL', FALSE, NULL, :days)
            ON CONFLICT (object_name, column_name) DO UPDATE SET
                classification = EXCLUDED.classification,
                encryption_required = EXCLUDED.encryption_required,
                masking_policy = EXCLUDED.masking_policy,
                retention_days = EXCLUDED.retention_days;
        """), {"days": retention_days})
        connection.execute(text("""
            CREATE OR REPLACE VIEW gold.dim_customers_masked AS
            SELECT customer_key,
                   encode(digest(customer_id::TEXT, 'sha256'), 'hex') AS customer_id_masked,
                   CASE WHEN customer_name IS NULL THEN NULL
                        ELSE LEFT(customer_name, 1) || '***' END AS customer_name_masked,
                   segment, ingested_at
            FROM gold.dim_customers;
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.encrypted_audit_values (
                audit_key TEXT PRIMARY KEY, encrypted_value BYTEA NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """))
        connection.execute(text("""
            INSERT INTO control.encrypted_audit_values (audit_key, encrypted_value)
            VALUES ('pipeline-governance-check', pgp_sym_encrypt('enabled', :key))
            ON CONFLICT (audit_key) DO UPDATE SET
                encrypted_value = EXCLUDED.encrypted_value,
                created_at = CURRENT_TIMESTAMP;
        """), {"key": os.getenv("AUDIT_ENCRYPTION_KEY", "retailion-local-demo-key")})
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.access_policies (
                role_name TEXT PRIMARY KEY, responsibility TEXT NOT NULL,
                allowed_objects TEXT[] NOT NULL, denied_objects TEXT[] NOT NULL,
                owner TEXT NOT NULL, reviewed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO control.access_policies
                (role_name, responsibility, allowed_objects, denied_objects, owner)
            VALUES
                ('retailion_bi_reader', 'consume masked analytics',
                 ARRAY['gold.dim_customers_masked', 'gold.fact_sales'],
                 ARRAY['gold.dim_customers', 'bronze.superstore'], 'security-compliance'),
                ('retailion_data_engineer', 'operate pipeline',
                 ARRAY['bronze', 'silver', 'gold', 'control'], ARRAY[]::TEXT[], 'platform-owner')
            ON CONFLICT (role_name) DO UPDATE SET
                responsibility = EXCLUDED.responsibility,
                allowed_objects = EXCLUDED.allowed_objects,
                denied_objects = EXCLUDED.denied_objects,
                owner = EXCLUDED.owner,
                reviewed_at = CURRENT_TIMESTAMP;
            CREATE TABLE IF NOT EXISTS control.audit_events (
                event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                event_type TEXT NOT NULL, actor TEXT NOT NULL,
                object_name TEXT NOT NULL, run_id UUID,
                event_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS control.sod_policies (
                policy_name TEXT PRIMARY KEY, conflicting_roles TEXT[] NOT NULL,
                enforced BOOLEAN NOT NULL DEFAULT TRUE,
                owner TEXT NOT NULL
            );
            INSERT INTO control.sod_policies
                (policy_name, conflicting_roles, owner)
            VALUES ('pipeline-and-approval-separation',
                    ARRAY['retailion_data_engineer', 'retailion_bi_reader'],
                    'security-compliance')
            ON CONFLICT (policy_name) DO UPDATE SET
                conflicting_roles = EXCLUDED.conflicting_roles,
                enforced = TRUE, owner = EXCLUDED.owner;
        """))
        connection.execute(text("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'retailion_bi_reader') THEN
                    GRANT USAGE ON SCHEMA gold TO retailion_bi_reader;
                    GRANT SELECT ON gold.dim_customers_masked, gold.fact_sales TO retailion_bi_reader;
                    REVOKE ALL ON gold.dim_customers FROM retailion_bi_reader;
                    REVOKE ALL ON bronze.superstore FROM retailion_bi_reader;
                END IF;
            END $$;
        """))
        connection.execute(text("""
            INSERT INTO control.audit_events (event_type, actor, object_name)
            VALUES ('SECURITY_GOVERNANCE_APPLIED', CURRENT_USER, 'retailion warehouse');
        """))
        retention_columns = {
            "pipeline_metrics": "recorded_at",
            "pipeline_traces": "started_at",
            "data_lineage": "recorded_at",
            "incident_evidence": "generated_at",
        }
        for table, timestamp_column in retention_columns.items():
            connection.execute(text(
                f"DELETE FROM control.{table} WHERE {timestamp_column} < CURRENT_TIMESTAMP - (:days * INTERVAL '1 day')"
            ), {"days": retention_days})
            connection.execute(text("""
            DELETE FROM control.pipeline_runs
            WHERE started_at < CURRENT_TIMESTAMP - (:days * INTERVAL '1 day')
        """), {"days": retention_days})


def ensure_monitoring_view(engine) -> None:
    """Expose one operational view for incident triage and run health."""
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE OR REPLACE VIEW control.pipeline_monitoring AS
            SELECT r.run_id,
                   r.pipeline_name,
                   r.status,
                   r.started_at,
                   r.finished_at,
                   CASE WHEN r.status = 'FAILED' THEN 'pipeline' ELSE NULL END AS failed_what,
                   CASE WHEN r.status = 'FAILED' THEN COALESCE(e.failure_stage, 'pipeline') ELSE NULL END AS failed_where,
                   CASE WHEN r.status = 'FAILED' THEN COALESCE(e.failed_at, r.finished_at) ELSE NULL END AS failed_since,
                   CASE WHEN r.status = 'FAILED' THEN COALESCE(e.impact, 'No active failure') ELSE 'No active failure' END AS impact,
                   CASE WHEN r.status = 'FAILED' THEN COALESCE(e.owner, 'data-engineering') ELSE NULL END AS owner,
                   CASE WHEN r.status = 'FAILED' THEN COALESCE(e.safe_next_action, 'Inspect the run and retry after correction') ELSE 'No action required' END AS safe_next_action,
                   r.error_message
            FROM control.pipeline_runs r
            LEFT JOIN control.incident_evidence e ON e.run_id = r.run_id;
        """))


def run_gold(engine) -> int:
    sql = """
    CREATE SCHEMA IF NOT EXISTS gold;
    DROP VIEW IF EXISTS gold.dim_customers_masked;
    DROP TABLE IF EXISTS gold.fact_sales CASCADE;
    DROP TABLE IF EXISTS gold.sales_daily;
    DROP TABLE IF EXISTS gold.sales_monthly;
    DROP TABLE IF EXISTS gold.fact_order_fulfillment;
    DROP TABLE IF EXISTS gold.dim_date;
    DROP TABLE IF EXISTS gold.dim_location;
    DROP TABLE IF EXISTS gold.dim_products;
    DROP TABLE IF EXISTS gold.dim_customers;

    CREATE TABLE gold.dim_customers AS
    SELECT ROW_NUMBER() OVER (ORDER BY customer_id)::INTEGER AS customer_key,
           customer_id, customer_name, segment, ingested_at
    FROM (
        SELECT DISTINCT ON (customer_id) customer_id, customer_name, segment, ingested_at
        FROM silver.superstore
        ORDER BY customer_id, ingested_at DESC
    ) latest_customers;
    ALTER TABLE gold.dim_customers ADD PRIMARY KEY (customer_key);
    ALTER TABLE gold.dim_customers ADD CONSTRAINT uq_dim_customers_business_key UNIQUE (customer_id);

    CREATE TABLE IF NOT EXISTS gold.dim_customers_scd2 (
        customer_key BIGSERIAL PRIMARY KEY,
        customer_id TEXT NOT NULL,
        customer_name TEXT,
        segment TEXT,
        valid_from TIMESTAMPTZ NOT NULL,
        valid_to TIMESTAMPTZ,
        is_current BOOLEAN NOT NULL,
        UNIQUE (customer_id, valid_from)
    );
    ALTER TABLE gold.dim_customers_scd2
        ADD COLUMN IF NOT EXISTS customer_key BIGINT;
    CREATE SEQUENCE IF NOT EXISTS gold.dim_customers_scd2_customer_key_seq;
    ALTER TABLE gold.dim_customers_scd2
        ALTER COLUMN customer_key SET DEFAULT
            nextval('gold.dim_customers_scd2_customer_key_seq');
    SELECT setval(
        'gold.dim_customers_scd2_customer_key_seq',
        COALESCE((SELECT MAX(customer_key) FROM gold.dim_customers_scd2), 0) + 1,
        FALSE
    );
    UPDATE gold.dim_customers_scd2 history
    SET valid_to = CURRENT_TIMESTAMP,
        is_current = FALSE
    FROM gold.dim_customers current_dim
    WHERE history.customer_id = current_dim.customer_id
      AND history.is_current
      AND (history.customer_name, history.segment)
          IS DISTINCT FROM (current_dim.customer_name, current_dim.segment);
    INSERT INTO gold.dim_customers_scd2
        (customer_id, customer_name, segment, valid_from, valid_to, is_current)
    SELECT current_dim.customer_id, current_dim.customer_name, current_dim.segment,
           current_dim.ingested_at, NULL, TRUE
    FROM gold.dim_customers current_dim
    WHERE NOT EXISTS (
        SELECT 1
        FROM gold.dim_customers_scd2 history
        WHERE history.customer_id = current_dim.customer_id
          AND history.valid_from = current_dim.ingested_at
    );

    CREATE TABLE gold.dim_products AS
    SELECT ROW_NUMBER() OVER (ORDER BY product_id)::INTEGER AS product_key,
           product_id, product_name, category, sub_category, ingested_at
    FROM (
        SELECT DISTINCT ON (product_id) product_id, product_name, category, sub_category, ingested_at
        FROM silver.superstore
        ORDER BY product_id, ingested_at DESC
    ) latest_products;
    ALTER TABLE gold.dim_products ADD PRIMARY KEY (product_key);
    ALTER TABLE gold.dim_products ADD CONSTRAINT uq_dim_products_business_key UNIQUE (product_id);

    CREATE TABLE gold.dim_location AS
    SELECT ROW_NUMBER() OVER (ORDER BY country, region, state, city, postal_code)::INTEGER AS location_id,
           country, region, state, city, postal_code
    FROM (SELECT DISTINCT country, region, state, city, postal_code FROM silver.superstore) locations;
    ALTER TABLE gold.dim_location ADD PRIMARY KEY (location_id);

    CREATE TABLE gold.dim_date AS
    WITH dates AS (
        SELECT order_date AS date_key FROM silver.superstore
        UNION
        SELECT ship_date FROM silver.superstore
    )
    SELECT date_key, EXTRACT(YEAR FROM date_key)::INTEGER AS year,
           EXTRACT(MONTH FROM date_key)::INTEGER AS month,
           EXTRACT(DAY FROM date_key)::INTEGER AS day,
           EXTRACT(QUARTER FROM date_key)::INTEGER AS quarter,
           TRIM(TO_CHAR(date_key, 'Day')) AS day_name,
           TRIM(TO_CHAR(date_key, 'Month')) AS month_name,
           EXTRACT(DOW FROM date_key) IN (0, 6) AS is_weekend
    FROM dates WHERE date_key IS NOT NULL;
    ALTER TABLE gold.dim_date ADD PRIMARY KEY (date_key);

    CREATE TABLE gold.fact_sales_stage AS
    SELECT s.row_id, s.order_id, s.order_date, s.ship_date,
           c.customer_key, scd.customer_key AS scd2_customer_key,
           p.product_key, l.location_id,
           s.ship_mode, s.sales, s.quantity, s.discount, s.profit,
           s.run_id, s.ingested_at
    FROM silver.superstore s
    JOIN gold.dim_customers c ON s.customer_id = c.customer_id
    LEFT JOIN LATERAL (
        SELECT customer_key
        FROM gold.dim_customers_scd2 history
        WHERE history.customer_id = s.customer_id
          AND history.is_current
        ORDER BY history.valid_from DESC, history.customer_key DESC
        LIMIT 1
    ) scd ON TRUE
    JOIN gold.dim_products p ON s.product_id = p.product_id
    LEFT JOIN gold.dim_location l USING (country, region, state, city, postal_code);
    CREATE TABLE gold.fact_sales (LIKE gold.fact_sales_stage INCLUDING DEFAULTS)
        PARTITION BY RANGE (order_date);
    DO $$
    DECLARE year_value INTEGER;
    BEGIN
        FOR year_value IN
            SELECT DISTINCT EXTRACT(YEAR FROM order_date)::INTEGER
            FROM silver.superstore
            WHERE order_date IS NOT NULL
        LOOP
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS gold.fact_sales_y%s PARTITION OF gold.fact_sales FOR VALUES FROM (%L) TO (%L)',
                year_value,
                make_date(year_value, 1, 1),
                make_date(year_value + 1, 1, 1)
            );
        END LOOP;
    END $$;
    CREATE TABLE gold.fact_sales_default PARTITION OF gold.fact_sales DEFAULT;
    INSERT INTO gold.fact_sales SELECT * FROM gold.fact_sales_stage;
    DROP TABLE gold.fact_sales_stage;
    ALTER TABLE gold.fact_sales ADD PRIMARY KEY (row_id, order_date);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_customer_scd2 FOREIGN KEY (scd2_customer_key) REFERENCES gold.dim_customers_scd2(customer_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_product FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_location FOREIGN KEY (location_id) REFERENCES gold.dim_location(location_id);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_order_date FOREIGN KEY (order_date) REFERENCES gold.dim_date(date_key);
    ALTER TABLE gold.fact_sales ADD CONSTRAINT fk_fact_ship_date FOREIGN KEY (ship_date) REFERENCES gold.dim_date(date_key);
    CREATE INDEX idx_fact_customer ON gold.fact_sales(customer_key);
    CREATE INDEX idx_fact_product ON gold.fact_sales(product_key);
    CREATE INDEX idx_fact_location ON gold.fact_sales(location_id);
    CREATE INDEX idx_fact_order_date ON gold.fact_sales(order_date);
    COMMENT ON TABLE gold.fact_sales IS
        'Grain: one row per source transaction row_id, partitioned by order_date.';
    CREATE TABLE gold.sales_daily AS
    SELECT order_date, customer_key, product_key, location_id,
           SUM(sales) AS sales, SUM(quantity) AS quantity,
           SUM(profit) AS profit, COUNT(*) AS transaction_count
    FROM gold.fact_sales
    GROUP BY order_date, customer_key, product_key, location_id;
    COMMENT ON TABLE gold.sales_daily IS
        'Semantic serving grain: one row per order_date, customer, product and location.';
    CREATE TABLE gold.fact_order_fulfillment AS
    SELECT order_id,
           MIN(order_date) AS order_date,
           MAX(ship_date) AS ship_date,
           MAX(ship_date) - MIN(order_date) AS days_to_ship,
           COUNT(*) AS line_count,
           SUM(sales) AS sales,
           SUM(quantity) AS quantity,
           SUM(profit) AS profit
    FROM silver.superstore
    GROUP BY order_id;
    ALTER TABLE gold.fact_order_fulfillment ADD PRIMARY KEY (order_id);
    COMMENT ON TABLE gold.fact_order_fulfillment IS
        'Accumulating snapshot grain: one row per order lifecycle.';
    CREATE TABLE gold.sales_monthly AS
    SELECT DATE_TRUNC('month', order_date)::DATE AS month_key,
           customer_key, product_key, location_id,
           SUM(sales) AS sales, SUM(quantity) AS quantity,
           SUM(profit) AS profit, SUM(transaction_count) AS transaction_count
    FROM gold.sales_daily
    GROUP BY DATE_TRUNC('month', order_date)::DATE,
             customer_key, product_key, location_id;
    COMMENT ON TABLE gold.sales_monthly IS
        'Semantic serving grain: one row per month, customer, product and location.';
    """
    with engine.begin() as connection:
        connection.execute(text(sql))
        boundary_match = connection.execute(text("""
            SELECT ABS(COALESCE(f.sales, 0) - COALESCE(d.sales, 0)) <= 0.000001
               AND ABS(COALESCE(f.profit, 0) - COALESCE(d.profit, 0)) <= 0.000001
               AND COALESCE(f.quantity, 0) = COALESCE(d.quantity, 0)
               AND f.rows = d.rows
            FROM (SELECT SUM(sales) sales, SUM(profit) profit,
                         SUM(quantity) quantity, COUNT(*) rows
                  FROM gold.fact_sales) f
            CROSS JOIN
                 (SELECT SUM(sales) sales, SUM(profit) profit,
                         SUM(quantity) quantity, SUM(transaction_count) rows
                  FROM gold.sales_daily) d
        """)).scalar_one()
        if not boundary_match:
            raise RuntimeError(
                "Fact/serving reconciliation failed"
            )
        return connection.execute(text("SELECT COUNT(*) FROM gold.fact_sales")).scalar_one()


def validate(engine, silver_count: int, gold_count: int, run_id: str) -> None:
    with engine.connect() as connection:
        null_locations = connection.execute(text(
            "SELECT COUNT(*) = 0 FROM gold.fact_sales WHERE location_id IS NULL"
        )).scalar_one()
        valid_measures = connection.execute(text(
            "SELECT COUNT(*) = 0 FROM gold.fact_sales WHERE sales < 0 OR quantity <= 0 OR discount NOT BETWEEN 0 AND 1"
        )).scalar_one()
        totals_match = connection.execute(text("""
            SELECT ABS(COALESCE(s.sales, 0) - COALESCE(g.sales, 0)) <= 0.000001
               AND ABS(COALESCE(s.profit, 0) - COALESCE(g.profit, 0)) <= 0.000001
               AND COALESCE(s.quantity, 0) = COALESCE(g.quantity, 0)
            FROM (
                SELECT SUM(sales) AS sales, SUM(profit) AS profit, SUM(quantity) AS quantity
                FROM silver.superstore
            ) s
            CROSS JOIN (
                SELECT SUM(sales) AS sales, SUM(profit) AS profit, SUM(quantity) AS quantity
                FROM gold.fact_sales
            ) g
        """)).scalar_one()
        checks = {
        "silver/gold row count": silver_count == gold_count,
        "null location keys": null_locations,
        "invalid measures": valid_measures,
        "silver/gold totals": totals_match,
    }
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.data_contract_rules (
                rule_name TEXT PRIMARY KEY,
                contract_name TEXT NOT NULL,
                rule_type TEXT NOT NULL DEFAULT 'TECHNICAL'
                    CHECK (rule_type IN ('TECHNICAL', 'BUSINESS')),
                handling TEXT NOT NULL DEFAULT 'REJECT'
                    CHECK (handling IN ('QUARANTINE', 'REJECT', 'ALLOW')),
                tolerance NUMERIC NOT NULL DEFAULT 0,
                owner TEXT NOT NULL,
                severity TEXT NOT NULL CHECK (severity IN ('STOP', 'WARN', 'QUARANTINE')),
                rule_version TEXT NOT NULL,
                description TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            ALTER TABLE control.data_contract_rules
                ADD COLUMN IF NOT EXISTS rule_type TEXT NOT NULL DEFAULT 'TECHNICAL';
            ALTER TABLE control.data_contract_rules
                ADD COLUMN IF NOT EXISTS handling TEXT NOT NULL DEFAULT 'REJECT';
            ALTER TABLE control.data_contract_rules
                ADD COLUMN IF NOT EXISTS tolerance NUMERIC NOT NULL DEFAULT 0;
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'control.data_contract_rules'::regclass
                      AND conname = 'ck_data_contract_rules_type'
                ) THEN
                    ALTER TABLE control.data_contract_rules
                        ADD CONSTRAINT ck_data_contract_rules_type
                        CHECK (rule_type IN ('TECHNICAL', 'BUSINESS'));
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conrelid = 'control.data_contract_rules'::regclass
                      AND conname = 'ck_data_contract_rules_handling'
                ) THEN
                    ALTER TABLE control.data_contract_rules
                        ADD CONSTRAINT ck_data_contract_rules_handling
                        CHECK (handling IN ('QUARANTINE', 'REJECT', 'ALLOW'));
                END IF;
            END $$;
        """))
        connection.execute(text("""
            INSERT INTO control.data_contract_rules
                (rule_name, contract_name, rule_type, handling, tolerance, owner, severity, rule_version, description)
            VALUES
                ('silver/gold row count', 'superstore_sales',
                 'TECHNICAL', 'REJECT', 0,
                 'data-engineering', 'STOP', :version,
                 'Silver and Gold must contain the same number of rows'),
                ('null location keys', 'superstore_sales',
                 'TECHNICAL', 'REJECT', 0,
                 'data-engineering', 'STOP', :version,
                 'Published fact rows must resolve to a location key'),
                ('invalid measures', 'superstore_sales',
                 'BUSINESS', 'QUARANTINE', 0,
                 'data-quality', 'QUARANTINE', :version,
                 'Sales, quantity and discount must satisfy business constraints'),
                ('silver/gold totals', 'superstore_sales',
                 'BUSINESS', 'REJECT', 0.000001,
                 'finance-data-owner', 'STOP', :version,
                 'Silver and Gold measures must reconcile within tolerance')
            ON CONFLICT (rule_name) DO UPDATE SET
                rule_type = EXCLUDED.rule_type,
                handling = EXCLUDED.handling,
                tolerance = EXCLUDED.tolerance,
                owner = EXCLUDED.owner,
                severity = EXCLUDED.severity,
                rule_version = EXCLUDED.rule_version,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
        """), {"version": QUALITY_RULE_VERSION})
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.data_quality_results (
                run_id UUID, rule_name TEXT, passed BOOLEAN NOT NULL,
                rule_version TEXT NOT NULL DEFAULT '1.0.0',
                checked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, rule_name)
            )
        """))
        connection.execute(text("""
            ALTER TABLE control.data_quality_results
                ADD COLUMN IF NOT EXISTS rule_version TEXT NOT NULL DEFAULT '1.0.0'
        """))
        connection.execute(text("""
            INSERT INTO control.data_quality_results
                (run_id, rule_name, passed, rule_version)
            VALUES (:run_id, :rule_name, :passed, :version)
            ON CONFLICT (run_id, rule_name) DO UPDATE SET passed = EXCLUDED.passed,
                rule_version = EXCLUDED.rule_version,
                checked_at = CURRENT_TIMESTAMP
        """), [{"run_id": run_id, "rule_name": name, "passed": passed}
               | {"version": QUALITY_RULE_VERSION}
               for name, passed in checks.items()])
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.reconciliation_results (
                run_id UUID PRIMARY KEY, silver_rows BIGINT, gold_rows BIGINT,
                silver_sales NUMERIC, gold_sales NUMERIC,
                silver_profit NUMERIC, gold_profit NUMERIC,
                silver_quantity BIGINT, gold_quantity BIGINT,
                passed BOOLEAN NOT NULL, checked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS control.data_quality_scores (
                run_id UUID PRIMARY KEY,
                total_rules INTEGER NOT NULL,
                passed_rules INTEGER NOT NULL,
                quality_score NUMERIC(5, 2) NOT NULL
                    CHECK (quality_score BETWEEN 0 AND 100),
                quarantined_rows BIGINT NOT NULL DEFAULT 0,
                calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            INSERT INTO control.reconciliation_results
            SELECT :run_id, s.rows, g.rows, s.sales, g.sales, s.profit, g.profit,
                   s.quantity, g.quantity, :passed, CURRENT_TIMESTAMP
            FROM (SELECT COUNT(*) rows, SUM(sales) sales, SUM(profit) profit, SUM(quantity) quantity FROM silver.superstore) s,
                 (SELECT COUNT(*) rows, SUM(sales) sales, SUM(profit) profit, SUM(quantity) quantity FROM gold.fact_sales) g
        """), {"run_id": run_id, "passed": checks["silver/gold totals"]})
        connection.execute(text("""
            INSERT INTO control.data_quality_scores
                (run_id, total_rules, passed_rules, quality_score, quarantined_rows)
            VALUES (
                :run_id,
                :total_rules,
                :passed_rules,
                ROUND((CAST(:passed_rules AS NUMERIC) / NULLIF(:total_rules, 0)) * 100, 2),
                (SELECT COUNT(*) FROM quarantine.superstore_invalid
                 WHERE CAST(run_id AS TEXT) = CAST(:run_id AS TEXT))
            )
            ON CONFLICT (run_id) DO UPDATE SET
                total_rules = EXCLUDED.total_rules,
                passed_rules = EXCLUDED.passed_rules,
                quality_score = EXCLUDED.quality_score,
                quarantined_rows = EXCLUDED.quarantined_rows,
                calculated_at = CURRENT_TIMESTAMP
        """), {
            "run_id": run_id,
            "total_rules": len(checks),
            "passed_rules": sum(checks.values()),
        })
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        message = (
            f"Data quality checks failed under {QUALITY_FAILURE_MODE} policy "
            f"(version {QUALITY_RULE_VERSION}): " + ", ".join(failed)
        )
        if QUALITY_FAILURE_MODE == "WARN":
            LOGGER.warning(message)
        elif QUALITY_FAILURE_MODE == "QUARANTINE":
            LOGGER.error(message + "; invalid rows were routed to quarantine")
        else:
            raise DataQualityError(message)
    LOGGER.info("All data quality checks passed")


def run(source_path: Path, start_date=None, end_date=None, replay=False,
        load_mode: str = "full", overlap_days: int = 2,
        chunk_size: int | None = None, throttle_ms: int = 0) -> None:
    settings = Settings.from_env()
    engine = create_db_engine(settings)
    ensure_observability_tables(engine)
    run_id = new_run_id()
    started_at = datetime.now(timezone.utc)
    pipeline_name = "retailion_superstore"
    try:
        ensure_watermark_table(engine)
        if load_mode in {"append", "upsert"} and not replay and start_date is None:
            watermark = read_watermark(engine, pipeline_name)
            if watermark is not None:
                start_date = watermark - timedelta(days=overlap_days)
                LOGGER.info("Using watermark start date: %s", start_date)
        with engine.begin() as connection:
            # Serialize runs so concurrent retries/backfills cannot publish over
            # one another or observe an intermediate layer state.
            connection.execute(text(
                "SELECT pg_advisory_xact_lock(hashtext(:pipeline_name))"
            ), {"pipeline_name": pipeline_name})
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS control"))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS control.pipeline_runs (
                    run_id UUID PRIMARY KEY,
                    pipeline_name VARCHAR(100) NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL,
                    finished_at TIMESTAMPTZ,
                    status VARCHAR(20) NOT NULL,
                    bronze_rows INTEGER,
                    silver_rows INTEGER,
                    gold_rows INTEGER,
                    error_message TEXT
                )
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS control.pipeline_sla_results (
                    run_id UUID PRIMARY KEY,
                    sla_seconds INTEGER NOT NULL,
                    duration_seconds NUMERIC(12, 3) NOT NULL,
                    met_sla BOOLEAN NOT NULL,
                    measured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS control.pipeline_failures (
                    run_id UUID PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    failure_stage TEXT NOT NULL DEFAULT 'pipeline',
                    error_message TEXT NOT NULL,
                    routed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    acknowledged_at TIMESTAMPTZ
                )
            """))
            connection.execute(text("""
                CREATE OR REPLACE VIEW control.pipeline_slo_daily AS
                SELECT started_at::DATE AS run_date,
                       COUNT(*) AS total_runs,
                       COUNT(*) FILTER (WHERE status = 'SUCCESS') AS successful_runs,
                       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'SUCCESS')
                             / NULLIF(COUNT(*), 0), 2) AS success_rate
                FROM control.pipeline_runs
                GROUP BY started_at::DATE
            """))
            connection.execute(text("""
                INSERT INTO control.pipeline_runs (run_id, pipeline_name, started_at, status)
                VALUES (:run_id, :pipeline_name, :started_at, 'STARTED')
            """), {"run_id": run_id, "pipeline_name": pipeline_name, "started_at": started_at})
        ensure_monitoring_view(engine)
        # A replay/backfill is explicit: it bypasses any future watermark state
        # and is still bounded by the optional date window.
        bronze_count = run_bronze(
            engine, source_path, load_mode, run_id, chunk_size, throttle_ms
        )
        source_window_count = count_source_rows(engine, run_id, start_date, end_date)
        if source_window_count == 0 and load_mode in {"append", "upsert"} and not replay:
            with engine.connect() as connection:
                gold_exists = connection.execute(text(
                    "SELECT to_regclass('gold.fact_sales') IS NOT NULL"
                )).scalar_one()
                existing_gold_count = connection.execute(text(
                    "SELECT COUNT(*) FROM gold.fact_sales"
                )).scalar_one() if gold_exists else 0
            mark_pipeline_run(
                engine, run_id, "NOOP", bronze_count, 0, existing_gold_count or 0
            )
            LOGGER.info(
                "No source rows after watermark; pipeline completed as NOOP run_id=%s",
                run_id,
            )
            return
        # Bronze is the accumulated source-of-record for append/upsert. Rebuild
        # Silver from all retained Bronze rows so incremental ingestion does not
        # discard historical facts.
        silver_start_date = None if load_mode in {"append", "upsert"} else start_date
        silver_end_date = None if load_mode in {"append", "upsert"} else end_date
        silver_count = run_silver(
            engine, run_id, source_path.name, silver_start_date, silver_end_date
        )
        gold_count = run_gold(engine)
        validate(engine, silver_count, gold_count, run_id)
        with engine.connect() as connection:
            max_date = connection.execute(text(
                """SELECT MAX(order_date)::TIMESTAMP AT TIME ZONE 'UTC'
                   FROM silver.superstore"""
            )).scalar_one()
        finished_at = datetime.now(timezone.utc)
        with engine.begin() as connection:
            duration_seconds = (finished_at - started_at).total_seconds()
            connection.execute(text("""
                INSERT INTO control.pipeline_sla_results
                    (run_id, sla_seconds, duration_seconds, met_sla)
                VALUES (:run_id, :sla_seconds, :duration_seconds,
                        :duration_seconds <= :sla_seconds)
                ON CONFLICT (run_id) DO UPDATE SET
                    duration_seconds = EXCLUDED.duration_seconds,
                    met_sla = EXCLUDED.met_sla,
                    measured_at = CURRENT_TIMESTAMP
            """), {"run_id": run_id, "sla_seconds": PIPELINE_SLA_SECONDS,
                   "duration_seconds": duration_seconds})
            if not replay and max_date is not None:
                connection.execute(text("""
                    INSERT INTO control.pipeline_watermarks
                        (pipeline_name, watermark_date)
                    VALUES (:pipeline_name, :watermark_date)
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        watermark_date = GREATEST(
                            control.pipeline_watermarks.watermark_date,
                            EXCLUDED.watermark_date
                        ),
                        updated_at = CURRENT_TIMESTAMP
                """), {"pipeline_name": pipeline_name, "watermark_date": max_date})
            connection.execute(text("""
                UPDATE control.pipeline_runs
                SET finished_at = :finished_at, status = 'SUCCESS',
                    bronze_rows = :bronze_rows, silver_rows = :silver_rows, gold_rows = :gold_rows
                WHERE run_id = :run_id
            """), {"finished_at": datetime.now(timezone.utc), "bronze_rows": bronze_count,
                   "silver_rows": silver_count, "gold_rows": gold_count, "run_id": run_id})
        publish_observability(engine, run_id, bronze_count, silver_count,
                              gold_count, started_at, finished_at)
        apply_security_governance(
            engine, int(os.getenv("DATA_RETENTION_DAYS", "90"))
        )
        LOGGER.info("Pipeline completed run_id=%s bronze=%s silver=%s gold=%s", run_id, bronze_count, silver_count, gold_count)
    except Exception as error:
        try:
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE control.pipeline_runs
                    SET finished_at = :finished_at, status = 'FAILED', error_message = :error_message
                    WHERE run_id = :run_id
                """), {"finished_at": datetime.now(timezone.utc), "error_message": str(error), "run_id": run_id})
                connection.execute(text("""
                    INSERT INTO control.pipeline_failures
                        (run_id, pipeline_name, error_message)
                    VALUES (:run_id, :pipeline_name, :error_message)
                    ON CONFLICT (run_id) DO UPDATE SET
                        error_message = EXCLUDED.error_message,
                        routed_at = CURRENT_TIMESTAMP
                """), {"run_id": run_id, "pipeline_name": pipeline_name,
                       "error_message": str(error)})
                connection.execute(text("""
                    INSERT INTO control.incident_evidence
                        (run_id, pipeline_name, failure_stage, failed_at,
                         error_message, impact, owner, safe_next_action)
                    VALUES (:run_id, :pipeline_name, 'pipeline', CURRENT_TIMESTAMP,
                            :error_message,
                            'Gold publish may be incomplete; downstream consumers must verify the latest successful run.',
                            'data-engineering',
                            'Inspect pipeline_failures and database availability, correct the cause, then rerun the bounded window with --replay.')
                    ON CONFLICT (run_id) DO UPDATE SET
                        error_message = EXCLUDED.error_message,
                        failed_at = EXCLUDED.failed_at,
                        generated_at = CURRENT_TIMESTAMP
                """), {"run_id": run_id, "pipeline_name": pipeline_name,
                        "error_message": str(error)})
        except Exception:
            LOGGER.exception("Could not persist failed pipeline status run_id=%s", run_id)
        LOGGER.exception("Pipeline failed run_id=%s", run_id)
        raise
    finally:
        engine.dispose()
