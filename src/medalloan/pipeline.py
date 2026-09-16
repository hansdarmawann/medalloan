"""Medalloan CSV-to-PostgreSQL pipeline for loan applications."""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from .config import Settings
from .database import create_db_engine, ensure_database_exists

LOGGER = logging.getLogger(__name__)


class PipelineError(RuntimeError):
    """Base class for expected pipeline failures."""


class DataQualityError(PipelineError):
    """Raised when a blocking data-quality rule fails."""


PIPELINE_SLA_SECONDS = int(os.getenv("PIPELINE_SLA_SECONDS", "3600"))
PIPELINE_ADVISORY_LOCK_KEY = 7_137_620_241
REQUIRED_SOURCE_COLUMNS = {
    "Loan_ID", "Gender", "Married", "Dependents", "Education", "Self_Employed",
    "ApplicantIncome", "CoapplicantIncome", "LoanAmount", "Loan_Amount_Term",
    "Credit_History", "Property_Area", "Loan_Status",
}


def validate_source_schema(columns) -> None:
    """Fail fast when the loan-application source contract changes."""
    missing = REQUIRED_SOURCE_COLUMNS.difference(columns)
    if missing:
        raise ValueError("Source schema validation failed; missing columns: " + ", ".join(sorted(missing)))


def new_run_id() -> str:
    timestamp_ms = time.time_ns() // 1_000_000
    value = (timestamp_ms << 80) | (0x7 << 76) | secrets.randbits(76)
    value = (value & ~(0b11 << 62)) | (0b10 << 62)
    return str(uuid.UUID(int=value))


def bronze_stage_table_name(run_id: str | None) -> str:
    """Return a per-run Bronze staging table name safe for PostgreSQL identifiers."""
    suffix = uuid.UUID(run_id).hex if run_id else uuid.uuid4().hex
    return f"loan_applications_stage_{suffix}"


def acquire_pipeline_lock(engine):
    """Reserve this database's shared warehouse tables for one pipeline run."""
    connection = engine.connect()
    try:
        acquired = connection.execute(text("""
            SELECT pg_try_advisory_lock(:lock_key)
        """), {"lock_key": PIPELINE_ADVISORY_LOCK_KEY}).scalar_one()
        if not acquired:
            raise PipelineError(
                "Another Medalloan pipeline run is active for this database; retry after it completes"
            )
        return connection
    except Exception:
        connection.close()
        raise


def release_pipeline_lock(connection) -> None:
    """Release a session-level lock and return its connection to the pool."""
    try:
        connection.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {
            "lock_key": PIPELINE_ADVISORY_LOCK_KEY,
        })
    except Exception:
        LOGGER.exception("Could not explicitly release the pipeline advisory lock")
    finally:
        connection.close()


def source_files(source_path: Path) -> list[Path]:
    """Return one source file or the eight loan-data parts in a directory."""
    if source_path.is_file():
        return [source_path]
    if source_path.is_dir():
        files = sorted(source_path.glob("loan_data_*.csv"))
        if files:
            return files
    raise FileNotFoundError(f"No loan CSV files found at {source_path}")


def read_source(source_path: Path, chunk_size: int | None = None, throttle_ms: int = 0) -> tuple[pd.DataFrame, str]:
    files = source_files(source_path)
    frames: list[pd.DataFrame] = []
    fingerprints: list[str] = []
    for file in files:
        before = hashlib.sha256(file.read_bytes()).hexdigest()
        if chunk_size and chunk_size > 0:
            chunks = []
            for chunk in pd.read_csv(file, chunksize=chunk_size):
                chunks.append(chunk)
                if throttle_ms > 0:
                    time.sleep(throttle_ms / 1000)
            frame = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        else:
            frame = pd.read_csv(file)
        after = hashlib.sha256(file.read_bytes()).hexdigest()
        if before != after:
            raise PipelineError(f"Source changed during extraction: {file}")
        validate_source_schema(frame.columns)
        frame["source_file"] = file.name
        frames.append(frame)
        fingerprints.append(before)
    return pd.concat(frames, ignore_index=True), hashlib.sha256("".join(fingerprints).encode()).hexdigest()


def ensure_control_tables(engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE SCHEMA IF NOT EXISTS control;
            CREATE TABLE IF NOT EXISTS control.pipeline_runs (
                run_id UUID PRIMARY KEY, pipeline_name TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ,
                status TEXT NOT NULL, bronze_rows INTEGER, silver_rows INTEGER,
                gold_rows INTEGER, error_message TEXT
            );
            CREATE TABLE IF NOT EXISTS control.source_schema_registry (
                source_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL,
                columns TEXT NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS control.cdc_events (
                run_id UUID NOT NULL, loan_id TEXT NOT NULL, operation TEXT NOT NULL,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, loan_id, operation)
            );
            CREATE TABLE IF NOT EXISTS control.data_quality_results (
                run_id UUID NOT NULL, rule_name TEXT NOT NULL, passed BOOLEAN NOT NULL,
                rule_version TEXT NOT NULL, checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, rule_name)
            );
            CREATE TABLE IF NOT EXISTS control.pipeline_metrics (
                run_id UUID NOT NULL, metric_name TEXT NOT NULL, metric_value NUMERIC NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (run_id, metric_name)
            );
        """))


def run_bronze(engine, source_path: Path, load_mode: str = "full", run_id: str | None = None,
               chunk_size: int | None = None, throttle_ms: int = 0) -> int:
    if load_mode not in {"full", "append", "upsert", "snapshot"}:
        raise ValueError(f"Unsupported load mode: {load_mode}")
    frame, source_hash = read_source(source_path, chunk_size, throttle_ms)
    if frame["Loan_ID"].isna().any() or (frame["Loan_ID"].astype(str).str.strip() == "").any():
        raise PipelineError("Loan_ID must be present for every source row")
    frame["Loan_ID"] = frame["Loan_ID"].astype(str).str.strip()
    frame = frame.drop_duplicates(subset=["Loan_ID"], keep="last")
    ensure_control_tables(engine)
    stage_table = bronze_stage_table_name(run_id)
    schema_columns = ",".join(frame.columns)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        connection.execute(text("""
            INSERT INTO control.source_schema_registry (source_name, schema_hash, columns)
            VALUES ('loan_data_parts', :schema_hash, :columns)
            ON CONFLICT (source_name) DO UPDATE SET schema_hash = EXCLUDED.schema_hash,
                columns = EXCLUDED.columns, observed_at = CURRENT_TIMESTAMP
        """), {"schema_hash": source_hash, "columns": schema_columns})
    try:
        frame.to_sql(stage_table, engine, schema="bronze", if_exists="fail", index=False)
        with engine.begin() as connection:
            stage = connection.dialect.identifier_preparer.quote_identifier(stage_table)
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS bronze.loan_applications
                    (LIKE bronze.{stage} INCLUDING DEFAULTS);
            """))
            if run_id:
                connection.execute(text(f"""
                    INSERT INTO control.cdc_events (run_id, loan_id, operation)
                    SELECT :run_id, incoming."Loan_ID", 'INSERT'
                    FROM bronze.{stage} incoming
                    WHERE NOT EXISTS (SELECT 1 FROM bronze.loan_applications current
                                      WHERE current."Loan_ID" = incoming."Loan_ID")
                    ON CONFLICT DO NOTHING;
                """), {"run_id": run_id})
            if run_id and load_mode != "append":
                connection.execute(text(f"""
                    INSERT INTO control.cdc_events (run_id, loan_id, operation)
                    SELECT :run_id, incoming."Loan_ID", 'UPDATE'
                    FROM bronze.{stage} incoming
                    JOIN bronze.loan_applications current ON current."Loan_ID" = incoming."Loan_ID"
                    WHERE md5(row_to_json(current)::text) <> md5(row_to_json(incoming)::text)
                    ON CONFLICT DO NOTHING;
                """), {"run_id": run_id})
            if run_id and load_mode in {"full", "snapshot"}:
                connection.execute(text(f"""
                    INSERT INTO control.cdc_events (run_id, loan_id, operation)
                    SELECT :run_id, current."Loan_ID", 'DELETE'
                    FROM bronze.loan_applications current
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bronze.{stage} incoming
                        WHERE incoming."Loan_ID" = current."Loan_ID"
                    )
                    ON CONFLICT DO NOTHING;
                """), {"run_id": run_id})
            if load_mode == "snapshot":
                snapshot = frame.assign(snapshot_run_id=run_id, snapshot_at=datetime.now(timezone.utc))
                snapshot.to_sql("loan_application_snapshots", engine, schema="bronze", if_exists="append", index=False)
            if load_mode in {"full", "snapshot"}:
                connection.execute(text("TRUNCATE TABLE bronze.loan_applications"))
                connection.execute(text(f"""
                    INSERT INTO bronze.loan_applications SELECT * FROM bronze.{stage};
                """))
            elif load_mode == "append":
                connection.execute(text(f"""
                    INSERT INTO bronze.loan_applications
                    SELECT incoming.*
                    FROM bronze.{stage} incoming
                    WHERE NOT EXISTS (
                        SELECT 1 FROM bronze.loan_applications current
                        WHERE current."Loan_ID" = incoming."Loan_ID"
                    );
                """))
            else:  # upsert
                connection.execute(text(f"""
                    DELETE FROM bronze.loan_applications current
                    USING bronze.{stage} incoming
                    WHERE current."Loan_ID" = incoming."Loan_ID"
                """))
                connection.execute(text(f"""
                    INSERT INTO bronze.loan_applications SELECT * FROM bronze.{stage};
                """))
            bronze_count = connection.execute(text(
                "SELECT COUNT(*) FROM bronze.loan_applications"
            )).scalar_one()
            connection.execute(text(f"DROP TABLE bronze.{stage}"))
        return bronze_count
    except Exception:
        with engine.begin() as connection:
            stage = connection.dialect.identifier_preparer.quote_identifier(stage_table)
            connection.execute(text(f"DROP TABLE IF EXISTS bronze.{stage}"))
        raise


def run_silver(engine, run_id: str, source_file: str = "loan_data_parts") -> int:
    stage_schema = "silver_stage_" + uuid.UUID(run_id).hex
    with engine.begin() as connection:
        schema = connection.dialect.identifier_preparer.quote_identifier(stage_schema)
        sql = f"""
            CREATE SCHEMA {schema};
            CREATE TABLE {schema}.loan_applications AS
            SELECT DISTINCT ON ("Loan_ID")
                TRIM("Loan_ID") AS loan_id,
                NULLIF(TRIM("Gender"), '') AS gender,
                NULLIF(TRIM("Married"), '') AS married,
                NULLIF(TRIM("Dependents"), '') AS dependents,
                NULLIF(TRIM("Education"), '') AS education,
                NULLIF(TRIM("Self_Employed"), '') AS self_employed,
                CAST("ApplicantIncome" AS NUMERIC(14,2)) AS applicant_income,
                CAST("CoapplicantIncome" AS NUMERIC(14,2)) AS coapplicant_income,
                CAST("LoanAmount" AS NUMERIC(14,2)) AS loan_amount,
                CAST("Loan_Amount_Term" AS INTEGER) AS loan_amount_term,
                CAST("Credit_History" AS NUMERIC(2,1)) AS credit_history,
                NULLIF(TRIM("Property_Area"), '') AS property_area,
                NULLIF(TRIM("Loan_Status"), '') AS loan_status,
                CURRENT_TIMESTAMP AS ingested_at,
                CAST(:run_id AS UUID) AS run_id,
                :source_file AS source_file,
                '2.0.0' AS pipeline_version
            FROM bronze.loan_applications
            ORDER BY "Loan_ID";
            ALTER TABLE {schema}.loan_applications ADD PRIMARY KEY (loan_id);

            CREATE SCHEMA IF NOT EXISTS silver;
            CREATE TABLE IF NOT EXISTS silver.loan_applications
                (LIKE {schema}.loan_applications INCLUDING ALL);
            TRUNCATE TABLE silver.loan_applications;
            INSERT INTO silver.loan_applications SELECT * FROM {schema}.loan_applications;

            CREATE SCHEMA IF NOT EXISTS quarantine;
            CREATE TABLE IF NOT EXISTS quarantine.loan_applications_invalid AS
            SELECT *, CAST(NULL AS TEXT) AS reason
            FROM {schema}.loan_applications WHERE FALSE;
            TRUNCATE TABLE quarantine.loan_applications_invalid;
            INSERT INTO quarantine.loan_applications_invalid
            SELECT *, CASE
                WHEN applicant_income < 0 OR coapplicant_income < 0 THEN 'negative income'
                WHEN loan_amount <= 0 THEN 'loan amount must be positive'
                WHEN loan_amount_term <= 0 THEN 'loan term must be positive'
                WHEN loan_status NOT IN ('Y', 'N') THEN 'invalid loan status'
            END AS reason
            FROM {schema}.loan_applications
            WHERE applicant_income < 0 OR coapplicant_income < 0 OR loan_amount <= 0
               OR loan_amount_term <= 0 OR loan_status NOT IN ('Y', 'N');
            DROP TABLE {schema}.loan_applications;
            DROP SCHEMA {schema};
        """
        connection.execute(text(sql), {"run_id": run_id, "source_file": source_file})
        return connection.execute(text("SELECT COUNT(*) FROM silver.loan_applications")).scalar_one()


def build_gold_candidate(connection, staging_schema: str) -> int:
    """Build unpublished Gold tables inside the caller's transaction."""
    schema = connection.dialect.identifier_preparer.quote_identifier(staging_schema)
    sql = f"""
        CREATE SCHEMA {schema};

        CREATE TABLE {schema}.dim_applicant_profile AS
        SELECT ROW_NUMBER() OVER (ORDER BY gender, married, dependents, education, self_employed)::INTEGER AS applicant_profile_key,
               gender, married, dependents, education, self_employed
        FROM (SELECT DISTINCT gender, married, dependents, education, self_employed
              FROM silver.loan_applications) profiles;
        ALTER TABLE {schema}.dim_applicant_profile ADD PRIMARY KEY (applicant_profile_key);

        CREATE TABLE {schema}.dim_property_area AS
        SELECT ROW_NUMBER() OVER (ORDER BY property_area)::INTEGER AS property_area_key, property_area
        FROM (SELECT DISTINCT property_area FROM silver.loan_applications) areas;
        ALTER TABLE {schema}.dim_property_area ADD PRIMARY KEY (property_area_key);
        ALTER TABLE {schema}.dim_property_area ADD CONSTRAINT uq_property_area UNIQUE (property_area);

        CREATE TABLE {schema}.dim_loan_status AS
        SELECT ROW_NUMBER() OVER (ORDER BY loan_status)::INTEGER AS loan_status_key, loan_status,
               CASE loan_status WHEN 'Y' THEN 'Approved' WHEN 'N' THEN 'Rejected' ELSE 'Unknown' END AS status_label
        FROM (SELECT DISTINCT loan_status FROM silver.loan_applications) statuses;
        ALTER TABLE {schema}.dim_loan_status ADD PRIMARY KEY (loan_status_key);
        ALTER TABLE {schema}.dim_loan_status ADD CONSTRAINT uq_loan_status UNIQUE (loan_status);

        CREATE TABLE {schema}.fact_loan_applications AS
        SELECT s.loan_id, p.applicant_profile_key, a.property_area_key, st.loan_status_key,
               s.applicant_income, s.coapplicant_income, s.loan_amount, s.loan_amount_term,
               s.credit_history, s.run_id, s.ingested_at
        FROM silver.loan_applications s
        JOIN {schema}.dim_applicant_profile p ON (p.gender, p.married, p.dependents, p.education, p.self_employed)
            IS NOT DISTINCT FROM (s.gender, s.married, s.dependents, s.education, s.self_employed)
        LEFT JOIN {schema}.dim_property_area a ON a.property_area IS NOT DISTINCT FROM s.property_area
        LEFT JOIN {schema}.dim_loan_status st ON st.loan_status IS NOT DISTINCT FROM s.loan_status;
        ALTER TABLE {schema}.fact_loan_applications ADD PRIMARY KEY (loan_id);
        ALTER TABLE {schema}.fact_loan_applications ADD CONSTRAINT fk_loan_profile FOREIGN KEY (applicant_profile_key)
            REFERENCES {schema}.dim_applicant_profile(applicant_profile_key);
        ALTER TABLE {schema}.fact_loan_applications ADD CONSTRAINT fk_loan_area FOREIGN KEY (property_area_key)
            REFERENCES {schema}.dim_property_area(property_area_key);
        ALTER TABLE {schema}.fact_loan_applications ADD CONSTRAINT fk_loan_status FOREIGN KEY (loan_status_key)
            REFERENCES {schema}.dim_loan_status(loan_status_key);

        CREATE TABLE {schema}.loan_approval_summary AS
        SELECT a.property_area, st.loan_status, COUNT(*) AS application_count,
               ROUND(AVG(f.loan_amount), 2) AS average_loan_amount,
               ROUND(AVG(f.applicant_income + f.coapplicant_income), 2) AS average_total_income
        FROM {schema}.fact_loan_applications f
        LEFT JOIN {schema}.dim_property_area a USING (property_area_key)
        LEFT JOIN {schema}.dim_loan_status st USING (loan_status_key)
        GROUP BY a.property_area, st.loan_status;
    """
    connection.execute(text(sql))
    return connection.execute(text(f"SELECT COUNT(*) FROM {schema}.fact_loan_applications")).scalar_one()


def evaluate_gold_quality(connection, staging_schema: str,
                          silver_count: int, gold_count: int) -> dict[str, bool]:
    """Evaluate the existing rules on the uncommitted candidate tables."""
    schema = connection.dialect.identifier_preparer.quote_identifier(staging_schema)
    return {
        "silver/gold row count": silver_count == gold_count,
        "unique loan id": connection.execute(text(
            f"SELECT COUNT(*) = COUNT(DISTINCT loan_id) FROM {schema}.fact_loan_applications"
        )).scalar_one(),
        "valid loan measures": connection.execute(text(f"""
            SELECT COUNT(*) = 0 FROM {schema}.fact_loan_applications
            WHERE applicant_income < 0 OR coapplicant_income < 0 OR loan_amount <= 0 OR loan_amount_term <= 0
        """)).scalar_one(),
        "valid loan status": connection.execute(text(f"""
            SELECT COUNT(*) = 0 FROM {schema}.dim_loan_status WHERE loan_status NOT IN ('Y', 'N')
        """)).scalar_one(),
    }


def record_quality_results(engine, run_id: str, checks: dict[str, bool], rule_version: str) -> None:
    """Commit audit evidence independently so a rejected candidate cannot erase it."""
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO control.data_quality_results (run_id, rule_name, passed, rule_version)
            VALUES (:run_id, :rule_name, :passed, :rule_version)
            ON CONFLICT (run_id, rule_name) DO UPDATE SET passed = EXCLUDED.passed,
                rule_version = EXCLUDED.rule_version, checked_at = CURRENT_TIMESTAMP
        """), [{"run_id": run_id, "rule_name": name, "passed": passed,
                  "rule_version": rule_version} for name, passed in checks.items()])


def enforce_quality_policy(checks: dict[str, bool], failure_mode: str) -> None:
    failed = [name for name, passed in checks.items() if not passed]
    if failed and failure_mode == "STOP":
        raise DataQualityError("Data quality checks failed: " + ", ".join(failed))
    if failed:
        LOGGER.warning("Data quality checks failed under %s: %s", failure_mode, ", ".join(failed))


def cleanup_gold_staging_schema(engine, staging_schema: str) -> None:
    """Remove a candidate schema after its transaction has rolled back."""
    schema = engine.dialect.identifier_preparer.quote_identifier(staging_schema)
    with engine.begin() as connection:
        for table in ("dim_applicant_profile", "dim_property_area", "dim_loan_status",
                      "fact_loan_applications", "loan_approval_summary"):
            connection.execute(text(f"DROP TABLE IF EXISTS {schema}.{table}"))
        connection.execute(text(f"DROP SCHEMA IF EXISTS {schema}"))


def publish_gold(connection, staging_schema: str) -> None:
    """Publish Gold atomically while preserving existing table objects."""
    schema = connection.dialect.identifier_preparer.quote_identifier(staging_schema)
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
    target_tables = ("dim_applicant_profile", "dim_property_area", "dim_loan_status",
                     "fact_loan_applications", "loan_approval_summary")
    existing = connection.execute(text("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'gold' AND table_name = 'fact_loan_applications'
    """)).scalar_one()
    if not existing:
        # First publication moves the validated candidate tables into the serving schema.
        for table in target_tables:
            connection.execute(text(f"ALTER TABLE {schema}.{table} SET SCHEMA gold"))
        connection.execute(text(f"DROP SCHEMA {schema}"))
        return

    target_count = connection.execute(text("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'gold' AND table_name IN
            ('dim_applicant_profile', 'dim_property_area', 'dim_loan_status',
             'fact_loan_applications', 'loan_approval_summary')
    """)).scalar_one()
    if target_count != len(target_tables):
        raise PipelineError("Gold schema is incomplete; refusing partial table publication")

    # Truncate all related tables in one statement, preserving object identity,
    # grants, indexes, constraints, and dependent views.
    connection.execute(text("""
        TRUNCATE TABLE gold.loan_approval_summary, gold.fact_loan_applications,
            gold.dim_loan_status, gold.dim_property_area, gold.dim_applicant_profile;
    """))
    for table in target_tables:
        connection.execute(text(f"INSERT INTO gold.{table} SELECT * FROM {schema}.{table}"))
    for table in reversed(target_tables):
        connection.execute(text(f"DROP TABLE {schema}.{table}"))
    connection.execute(text(f"DROP SCHEMA {schema}"))


def run(source_path: Path, start_date=None, end_date=None, replay=False,
        load_mode: str = "full", overlap_days: int = 2,
        chunk_size: int | None = None, throttle_ms: int = 0) -> None:
    """Run the Medalloan pipeline. Date-window arguments are retained for CLI compatibility."""
    if start_date or end_date or replay or overlap_days != 2:
        LOGGER.info("Date-window and watermark options are not used because loan data has no event date.")
    settings = Settings.from_env()
    ensure_database_exists(settings)
    engine = create_db_engine(settings)
    lock_connection = None
    run_id = new_run_id()
    started_at = datetime.now(timezone.utc)
    pipeline_name = "medalloan_loan_applications"
    bronze_rows = silver_rows = gold_rows = 0
    run_started = False
    staging_schema = None
    try:
        lock_connection = acquire_pipeline_lock(engine)
        ensure_control_tables(engine)
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO control.pipeline_runs (run_id, pipeline_name, started_at, status)
                VALUES (:run_id, :pipeline_name, :started_at, 'STARTED')
            """), {"run_id": run_id, "pipeline_name": pipeline_name, "started_at": started_at})
        run_started = True
        bronze_rows = run_bronze(engine, source_path, load_mode, run_id, chunk_size, throttle_ms)
        silver_rows = run_silver(engine, run_id)
        staging_schema = "gold_stage_" + uuid.UUID(run_id).hex
        with engine.begin() as connection:
            gold_rows = build_gold_candidate(connection, staging_schema)
            checks = evaluate_gold_quality(connection, staging_schema, silver_rows, gold_rows)
            record_quality_results(engine, run_id, checks, settings.quality_rule_version)
            enforce_quality_policy(checks, settings.quality_failure_mode)
            publish_gold(connection, staging_schema)
            finished_at = datetime.now(timezone.utc)
            duration = (finished_at - started_at).total_seconds()
            connection.execute(text("""
                UPDATE control.pipeline_runs SET finished_at = :finished_at, status = 'SUCCESS',
                    bronze_rows = :bronze_rows, silver_rows = :silver_rows, gold_rows = :gold_rows
                WHERE run_id = :run_id;
                INSERT INTO control.pipeline_metrics (run_id, metric_name, metric_value)
                VALUES (:run_id, 'bronze_rows', :bronze_rows), (:run_id, 'silver_rows', :silver_rows),
                       (:run_id, 'gold_rows', :gold_rows), (:run_id, 'duration_seconds', :duration)
                ON CONFLICT (run_id, metric_name) DO UPDATE SET metric_value = EXCLUDED.metric_value,
                    recorded_at = CURRENT_TIMESTAMP;
            """), {"run_id": run_id, "finished_at": finished_at, "bronze_rows": bronze_rows,
                     "silver_rows": silver_rows, "gold_rows": gold_rows, "duration": duration})
        LOGGER.info("Pipeline completed run_id=%s bronze=%s silver=%s gold=%s", run_id, bronze_rows, silver_rows, gold_rows)
    except Exception as error:
        if staging_schema is not None:
            cleanup_gold_staging_schema(engine, staging_schema)
        if run_started:
            with engine.begin() as connection:
                connection.execute(text("""
                    UPDATE control.pipeline_runs SET finished_at = CURRENT_TIMESTAMP, status = 'FAILED',
                        error_message = :error_message WHERE run_id = :run_id
                """), {"run_id": run_id, "error_message": str(error)})
        LOGGER.exception("Pipeline failed run_id=%s", run_id)
        raise
    finally:
        if lock_connection is not None:
            release_pipeline_lock(lock_connection)
        engine.dispose()
