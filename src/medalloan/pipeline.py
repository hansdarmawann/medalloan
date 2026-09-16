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


QUALITY_FAILURE_MODE = os.getenv("QUALITY_FAILURE_MODE", "STOP").upper()
QUALITY_RULE_VERSION = os.getenv("QUALITY_RULE_VERSION", "1.0.0")
PIPELINE_SLA_SECONDS = int(os.getenv("PIPELINE_SLA_SECONDS", "3600"))
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
    schema_columns = ",".join(frame.columns)
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        connection.execute(text("""
            INSERT INTO control.source_schema_registry (source_name, schema_hash, columns)
            VALUES ('loan_data_parts', :schema_hash, :columns)
            ON CONFLICT (source_name) DO UPDATE SET schema_hash = EXCLUDED.schema_hash,
                columns = EXCLUDED.columns, observed_at = CURRENT_TIMESTAMP
        """), {"schema_hash": source_hash, "columns": schema_columns})
        connection.execute(text("DROP TABLE IF EXISTS bronze.loan_applications_stage"))
    frame.to_sql("loan_applications_stage", engine, schema="bronze", if_exists="replace", index=False)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze.loan_applications
                (LIKE bronze.loan_applications_stage INCLUDING DEFAULTS);
        """))
        if run_id:
            connection.execute(text("""
                INSERT INTO control.cdc_events (run_id, loan_id, operation)
                SELECT :run_id, incoming."Loan_ID", 'INSERT'
                FROM bronze.loan_applications_stage incoming
                WHERE NOT EXISTS (SELECT 1 FROM bronze.loan_applications current
                                  WHERE current."Loan_ID" = incoming."Loan_ID")
                ON CONFLICT DO NOTHING;
                INSERT INTO control.cdc_events (run_id, loan_id, operation)
                SELECT :run_id, incoming."Loan_ID", 'UPDATE'
                FROM bronze.loan_applications_stage incoming
                JOIN bronze.loan_applications current ON current."Loan_ID" = incoming."Loan_ID"
                WHERE md5(row_to_json(current)::text) <> md5(row_to_json(incoming)::text)
                ON CONFLICT DO NOTHING;
            """), {"run_id": run_id})
        if load_mode == "snapshot":
            snapshot = frame.assign(snapshot_run_id=run_id, snapshot_at=datetime.now(timezone.utc))
            snapshot.to_sql("loan_application_snapshots", engine, schema="bronze", if_exists="append", index=False)
        if load_mode in {"full", "snapshot"}:
            connection.execute(text("TRUNCATE TABLE bronze.loan_applications"))
        else:
            connection.execute(text("""
                DELETE FROM bronze.loan_applications current
                USING bronze.loan_applications_stage incoming
                WHERE current."Loan_ID" = incoming."Loan_ID"
            """))
        connection.execute(text("""
            INSERT INTO bronze.loan_applications SELECT * FROM bronze.loan_applications_stage;
            DROP TABLE bronze.loan_applications_stage;
        """))
    return len(frame)


def run_silver(engine, run_id: str, source_file: str = "loan_data_parts") -> int:
    sql = """
        CREATE SCHEMA IF NOT EXISTS silver;
        DROP TABLE IF EXISTS silver.loan_applications;
        CREATE TABLE silver.loan_applications AS
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
        ALTER TABLE silver.loan_applications ADD PRIMARY KEY (loan_id);

        CREATE SCHEMA IF NOT EXISTS quarantine;
        DROP TABLE IF EXISTS quarantine.loan_applications_invalid;
        CREATE TABLE quarantine.loan_applications_invalid AS
        SELECT *, CASE
            WHEN applicant_income < 0 OR coapplicant_income < 0 THEN 'negative income'
            WHEN loan_amount <= 0 THEN 'loan amount must be positive'
            WHEN loan_amount_term <= 0 THEN 'loan term must be positive'
            WHEN loan_status NOT IN ('Y', 'N') THEN 'invalid loan status'
        END AS reason
        FROM silver.loan_applications
        WHERE applicant_income < 0 OR coapplicant_income < 0 OR loan_amount <= 0
           OR loan_amount_term <= 0 OR loan_status NOT IN ('Y', 'N');
    """
    with engine.begin() as connection:
        connection.execute(text(sql), {"run_id": run_id, "source_file": source_file})
        return connection.execute(text("SELECT COUNT(*) FROM silver.loan_applications")).scalar_one()


def run_gold(engine) -> int:
    sql = """
        CREATE SCHEMA IF NOT EXISTS gold;
        DROP TABLE IF EXISTS gold.loan_approval_summary;
        DROP TABLE IF EXISTS gold.fact_loan_applications;
        DROP TABLE IF EXISTS gold.dim_loan_status;
        DROP TABLE IF EXISTS gold.dim_property_area;
        DROP TABLE IF EXISTS gold.dim_applicant_profile;

        CREATE TABLE gold.dim_applicant_profile AS
        SELECT ROW_NUMBER() OVER (ORDER BY gender, married, dependents, education, self_employed)::INTEGER AS applicant_profile_key,
               gender, married, dependents, education, self_employed
        FROM (SELECT DISTINCT gender, married, dependents, education, self_employed
              FROM silver.loan_applications) profiles;
        ALTER TABLE gold.dim_applicant_profile ADD PRIMARY KEY (applicant_profile_key);

        CREATE TABLE gold.dim_property_area AS
        SELECT ROW_NUMBER() OVER (ORDER BY property_area)::INTEGER AS property_area_key, property_area
        FROM (SELECT DISTINCT property_area FROM silver.loan_applications) areas;
        ALTER TABLE gold.dim_property_area ADD PRIMARY KEY (property_area_key);
        ALTER TABLE gold.dim_property_area ADD CONSTRAINT uq_property_area UNIQUE (property_area);

        CREATE TABLE gold.dim_loan_status AS
        SELECT ROW_NUMBER() OVER (ORDER BY loan_status)::INTEGER AS loan_status_key, loan_status,
               CASE loan_status WHEN 'Y' THEN 'Approved' WHEN 'N' THEN 'Rejected' ELSE 'Unknown' END AS status_label
        FROM (SELECT DISTINCT loan_status FROM silver.loan_applications) statuses;
        ALTER TABLE gold.dim_loan_status ADD PRIMARY KEY (loan_status_key);
        ALTER TABLE gold.dim_loan_status ADD CONSTRAINT uq_loan_status UNIQUE (loan_status);

        CREATE TABLE gold.fact_loan_applications AS
        SELECT s.loan_id, p.applicant_profile_key, a.property_area_key, st.loan_status_key,
               s.applicant_income, s.coapplicant_income, s.loan_amount, s.loan_amount_term,
               s.credit_history, s.run_id, s.ingested_at
        FROM silver.loan_applications s
        JOIN gold.dim_applicant_profile p ON (p.gender, p.married, p.dependents, p.education, p.self_employed)
            IS NOT DISTINCT FROM (s.gender, s.married, s.dependents, s.education, s.self_employed)
        LEFT JOIN gold.dim_property_area a ON a.property_area IS NOT DISTINCT FROM s.property_area
        LEFT JOIN gold.dim_loan_status st ON st.loan_status IS NOT DISTINCT FROM s.loan_status;
        ALTER TABLE gold.fact_loan_applications ADD PRIMARY KEY (loan_id);
        ALTER TABLE gold.fact_loan_applications ADD CONSTRAINT fk_loan_profile FOREIGN KEY (applicant_profile_key)
            REFERENCES gold.dim_applicant_profile(applicant_profile_key);
        ALTER TABLE gold.fact_loan_applications ADD CONSTRAINT fk_loan_area FOREIGN KEY (property_area_key)
            REFERENCES gold.dim_property_area(property_area_key);
        ALTER TABLE gold.fact_loan_applications ADD CONSTRAINT fk_loan_status FOREIGN KEY (loan_status_key)
            REFERENCES gold.dim_loan_status(loan_status_key);

        CREATE TABLE gold.loan_approval_summary AS
        SELECT a.property_area, st.loan_status, COUNT(*) AS application_count,
               ROUND(AVG(f.loan_amount), 2) AS average_loan_amount,
               ROUND(AVG(f.applicant_income + f.coapplicant_income), 2) AS average_total_income
        FROM gold.fact_loan_applications f
        LEFT JOIN gold.dim_property_area a USING (property_area_key)
        LEFT JOIN gold.dim_loan_status st USING (loan_status_key)
        GROUP BY a.property_area, st.loan_status;
    """
    with engine.begin() as connection:
        connection.execute(text(sql))
        return connection.execute(text("SELECT COUNT(*) FROM gold.fact_loan_applications")).scalar_one()


def validate(engine, silver_count: int, gold_count: int, run_id: str) -> None:
    with engine.connect() as connection:
        checks = {
            "silver/gold row count": silver_count == gold_count,
            "unique loan id": connection.execute(text("SELECT COUNT(*) = COUNT(DISTINCT loan_id) FROM gold.fact_loan_applications")).scalar_one(),
            "valid loan measures": connection.execute(text("""
                SELECT COUNT(*) = 0 FROM gold.fact_loan_applications
                WHERE applicant_income < 0 OR coapplicant_income < 0 OR loan_amount <= 0 OR loan_amount_term <= 0
            """)).scalar_one(),
            "valid loan status": connection.execute(text("""
                SELECT COUNT(*) = 0 FROM gold.dim_loan_status WHERE loan_status NOT IN ('Y', 'N')
            """)).scalar_one(),
        }
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO control.data_quality_results (run_id, rule_name, passed, rule_version)
            VALUES (:run_id, :rule_name, :passed, :rule_version)
            ON CONFLICT (run_id, rule_name) DO UPDATE SET passed = EXCLUDED.passed,
                rule_version = EXCLUDED.rule_version, checked_at = CURRENT_TIMESTAMP
        """), [{"run_id": run_id, "rule_name": name, "passed": passed,
                  "rule_version": QUALITY_RULE_VERSION} for name, passed in checks.items()])
    failed = [name for name, passed in checks.items() if not passed]
    if failed and QUALITY_FAILURE_MODE == "STOP":
        raise DataQualityError("Data quality checks failed: " + ", ".join(failed))
    if failed:
        LOGGER.warning("Data quality checks failed under %s: %s", QUALITY_FAILURE_MODE, ", ".join(failed))


def run(source_path: Path, start_date=None, end_date=None, replay=False,
        load_mode: str = "full", overlap_days: int = 2,
        chunk_size: int | None = None, throttle_ms: int = 0) -> None:
    """Run the Medalloan pipeline. Date-window arguments are retained for CLI compatibility."""
    if start_date or end_date or replay or overlap_days != 2:
        LOGGER.info("Date-window and watermark options are not used because loan data has no event date.")
    settings = Settings.from_env()
    ensure_database_exists(settings)
    engine = create_db_engine(settings)
    run_id = new_run_id()
    started_at = datetime.now(timezone.utc)
    pipeline_name = "medalloan_loan_applications"
    bronze_rows = silver_rows = gold_rows = 0
    try:
        ensure_control_tables(engine)
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO control.pipeline_runs (run_id, pipeline_name, started_at, status)
                VALUES (:run_id, :pipeline_name, :started_at, 'STARTED')
            """), {"run_id": run_id, "pipeline_name": pipeline_name, "started_at": started_at})
        bronze_rows = run_bronze(engine, source_path, load_mode, run_id, chunk_size, throttle_ms)
        silver_rows = run_silver(engine, run_id)
        gold_rows = run_gold(engine)
        validate(engine, silver_rows, gold_rows, run_id)
        finished_at = datetime.now(timezone.utc)
        with engine.begin() as connection:
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
        with engine.begin() as connection:
            connection.execute(text("""
                UPDATE control.pipeline_runs SET finished_at = CURRENT_TIMESTAMP, status = 'FAILED',
                    error_message = :error_message WHERE run_id = :run_id
            """), {"run_id": run_id, "error_message": str(error)})
        LOGGER.exception("Pipeline failed run_id=%s", run_id)
        raise
    finally:
        engine.dispose()
