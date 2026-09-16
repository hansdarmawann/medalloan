"""Exercise publication and rollback against disposable PostgreSQL databases."""

from dataclasses import replace
import os
from pathlib import Path
import sys
import uuid

import pandas as pd
import pytest
from sqlalchemy import event, text

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medalloan import pipeline  # noqa: E402
from medalloan.config import Settings  # noqa: E402
from medalloan.database import create_db_engine  # noqa: E402


pytestmark = pytest.mark.integration
GOLD_TABLES = (
    "dim_applicant_profile", "dim_property_area", "dim_loan_status",
    "fact_loan_applications", "loan_approval_summary",
)


@pytest.fixture
def isolated_database(monkeypatch):
    if os.getenv("RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("Set RUN_POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")

    settings = Settings.from_env()
    database_name = "medalloan_test_" + uuid.uuid4().hex
    admin = create_db_engine(replace(settings, db_name="postgres"))
    database_identifier = admin.dialect.identifier_preparer.quote_identifier(database_name)
    engine = None
    created = False
    try:
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(text(f"CREATE DATABASE {database_identifier}"))
        created = True
        monkeypatch.setenv("DB_NAME", database_name)
        monkeypatch.setenv("QUALITY_FAILURE_MODE", "STOP")
        monkeypatch.setenv("QUALITY_RULE_VERSION", "publication-test-v1")
        engine = create_db_engine(Settings.from_env())
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        try:
            if created:
                # Only the uniquely named database successfully created above is removed.
                with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
                    connection.execute(text(f"DROP DATABASE {database_identifier}"))
        finally:
            admin.dispose()


@pytest.fixture
def source_csv(tmp_path):
    source = tmp_path / "loans.csv"
    # Keep a complete partition, including categorical values such as '3+'.
    source.write_bytes((ROOT / "data" / "loan_data_01.csv").read_bytes())
    return source


def gold_snapshot(engine):
    with engine.connect() as connection:
        return {
            table: connection.execute(text(
                f"SELECT row_to_json(t)::text FROM gold.{table} t ORDER BY row_to_json(t)::text"
            )).scalars().all()
            for table in GOLD_TABLES
        }


def latest_run(engine):
    with engine.connect() as connection:
        return connection.execute(text(
            "SELECT * FROM control.pipeline_runs ORDER BY started_at DESC LIMIT 1"
        )).mappings().one()


def assert_quality_audit(engine, run_id, failed_rules):
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT rule_name, passed, rule_version FROM control.data_quality_results
            WHERE run_id = :run_id
        """), {"run_id": run_id}).mappings().all()
    assert len(rows) == 4
    assert {row["rule_name"] for row in rows if not row["passed"]} == set(failed_rules)
    assert {row["rule_version"] for row in rows} == {"publication-test-v1"}


def assert_no_staging(engine):
    with engine.connect() as connection:
        assert connection.execute(text("""
            SELECT COUNT(*) FROM pg_namespace WHERE left(nspname, 11) = 'gold_stage_'
        """)).scalar_one() == 0


def assert_failed_without_metrics(engine):
    failed = latest_run(engine)
    assert failed["status"] == "FAILED"
    assert failed["finished_at"] is not None
    assert failed["error_message"]
    with engine.connect() as connection:
        assert connection.execute(text("""
            SELECT COUNT(*) FROM control.pipeline_metrics WHERE run_id = :run_id
        """), {"run_id": failed["run_id"]}).scalar_one() == 0
    assert_no_staging(engine)
    return failed


def test_valid_input_publishes_all_gold_tables_and_audit(isolated_database):
    engine = isolated_database
    pipeline.run(ROOT / "data")
    run = latest_run(engine)
    assert run["status"] == "SUCCESS"
    assert run["finished_at"] is not None
    assert run["bronze_rows"] == run["silver_rows"] == run["gold_rows"] == 381
    assert all(gold_snapshot(engine).values())
    assert_quality_audit(engine, run["run_id"], [])
    with engine.connect() as connection:
        metrics = dict(connection.execute(text("""
            SELECT metric_name, metric_value FROM control.pipeline_metrics WHERE run_id = :run_id
        """), {"run_id": run["run_id"]}).all())
        assert metrics["bronze_rows"] == metrics["silver_rows"] == metrics["gold_rows"] == 381
        assert metrics["duration_seconds"] >= 0
        assert connection.execute(text("""
            SELECT DISTINCT run_id FROM gold.fact_loan_applications
        """)).scalars().all() == [run["run_id"]]
        constraints = connection.execute(text("""
            SELECT c.contype FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE n.nspname = 'gold'
        """)).scalars().all()
        assert constraints.count("p") == 4
        assert constraints.count("f") == 3
        assert constraints.count("u") == 2
    assert_no_staging(engine)


@pytest.mark.parametrize("column,value,failed_rule", [
    ("ApplicantIncome", -1, "valid loan measures"),
    ("Loan_Status", "INVALID", "valid loan status"),
])
def test_stop_preserves_every_gold_table(isolated_database, source_csv, column, value, failed_rule):
    engine = isolated_database
    pipeline.run(source_csv)
    before = gold_snapshot(engine)
    frame = pd.read_csv(source_csv)
    frame.loc[0, column] = value
    frame.to_csv(source_csv, index=False)

    with pytest.raises(pipeline.DataQualityError, match=failed_rule):
        pipeline.run(source_csv)

    assert gold_snapshot(engine) == before
    failed = assert_failed_without_metrics(engine)
    assert_quality_audit(engine, failed["run_id"], [failed_rule])
    with engine.connect() as connection:
        assert connection.execute(text("""
            SELECT DISTINCT run_id FROM silver.loan_applications
        """)).scalars().all() == [failed["run_id"]]
        # Bronze retains the rejected source for investigation.
        observed = connection.execute(text(
            f'SELECT "{column}" FROM bronze.loan_applications WHERE "Loan_ID" = :loan_id'
        ), {"loan_id": frame.loc[0, "Loan_ID"]}).scalar_one()
        assert observed == value


def test_first_failed_run_leaves_no_gold_tables(isolated_database, source_csv):
    frame = pd.read_csv(source_csv)
    frame.loc[0, "ApplicantIncome"] = -1
    frame.to_csv(source_csv, index=False)
    with pytest.raises(pipeline.DataQualityError):
        pipeline.run(source_csv)
    failed = assert_failed_without_metrics(isolated_database)
    assert_quality_audit(isolated_database, failed["run_id"], ["valid loan measures"])
    with isolated_database.connect() as connection:
        assert connection.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'gold'
        """)).scalar_one() == 0


@pytest.mark.parametrize("failure_point", ["during_move", "after_success_update"])
def test_publication_failure_rolls_back_all_tables_and_success_metadata(
    isolated_database, source_csv, monkeypatch, failure_point,
):
    engine = isolated_database
    pipeline.run(source_csv)
    before = gold_snapshot(engine)
    frame = pd.read_csv(source_csv)
    frame.loc[0, "ApplicantIncome"] += 100
    frame.to_csv(source_csv, index=False)
    injected = []

    def fail_after_statement(connection, cursor, statement, parameters, context, executemany):
        during_move = statement.startswith("ALTER TABLE") and ".dim_property_area SET SCHEMA gold" in statement
        after_success_update = "status = 'SUCCESS'" in statement
        if (failure_point == "during_move" and during_move) or (
            failure_point == "after_success_update" and after_success_update
        ):
            injected.append(True)
            raise RuntimeError("Injected publication failure")

    monkeypatch.setattr(pipeline, "create_db_engine", lambda settings: engine)
    event.listen(engine, "after_cursor_execute", fail_after_statement)
    try:
        with pytest.raises(RuntimeError, match="Injected publication failure"):
            pipeline.run(source_csv)
    finally:
        event.remove(engine, "after_cursor_execute", fail_after_statement)

    assert injected == [True]
    assert gold_snapshot(engine) == before
    failed = assert_failed_without_metrics(engine)
    assert_quality_audit(engine, failed["run_id"], [])


@pytest.mark.parametrize("mode", ["WARN", "QUARANTINE"])
def test_nonblocking_policy_uses_runtime_settings_and_publishes(
    isolated_database, source_csv, monkeypatch, caplog, mode,
):
    pipeline.run(source_csv)
    before = gold_snapshot(isolated_database)
    frame = pd.read_csv(source_csv)
    frame.loc[0, "ApplicantIncome"] = -1
    frame.to_csv(source_csv, index=False)
    # The pipeline module was already imported: policy must use current Settings.
    monkeypatch.setenv("QUALITY_FAILURE_MODE", mode)
    pipeline.run(source_csv)
    run = latest_run(isolated_database)
    assert run["status"] == "SUCCESS"
    assert gold_snapshot(isolated_database) != before
    assert_quality_audit(isolated_database, run["run_id"], ["valid loan measures"])
    assert f"Data quality checks failed under {mode}" in caplog.text
    with isolated_database.connect() as connection:
        assert connection.execute(text("""
            SELECT COUNT(*) FROM gold.fact_loan_applications WHERE applicant_income < 0
        """)).scalar_one() == 1
    assert_no_staging(isolated_database)


def test_append_inserts_only_new_ids_and_is_idempotent(isolated_database, source_csv):
    pipeline.run(source_csv)
    frame = pd.read_csv(source_csv)
    existing_id = frame.loc[0, "Loan_ID"]
    original_income = int(frame.loc[0, "ApplicantIncome"])
    frame.loc[0, "ApplicantIncome"] = original_income + 1000
    new_row = frame.iloc[[1]].copy()
    new_row["Loan_ID"] = "LP_TEST_APPEND"
    batch = pd.concat([frame, new_row], ignore_index=True)
    batch.to_csv(source_csv, index=False)

    pipeline.run(source_csv, load_mode="append")
    append_run = latest_run(isolated_database)
    assert append_run["status"] == "SUCCESS"
    assert append_run["bronze_rows"] == append_run["silver_rows"] == append_run["gold_rows"] == 49
    with isolated_database.connect() as connection:
        assert connection.execute(text("""
            SELECT "ApplicantIncome" FROM bronze.loan_applications WHERE "Loan_ID" = :loan_id
        """), {"loan_id": existing_id}).scalar_one() == original_income
        assert connection.execute(text("""
            SELECT operation, loan_id FROM control.cdc_events
            WHERE run_id = :run_id ORDER BY operation, loan_id
        """), {"run_id": append_run["run_id"]}).all() == [("INSERT", "LP_TEST_APPEND")]

    pipeline.run(source_csv, load_mode="append")
    repeated_run = latest_run(isolated_database)
    with isolated_database.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM bronze.loan_applications"
        )).scalar_one() == 49
        assert connection.execute(text("""
            SELECT COUNT(*) FROM control.cdc_events WHERE run_id = :run_id
        """), {"run_id": repeated_run["run_id"]}).scalar_one() == 0


def test_upsert_updates_existing_ids_and_inserts_new_ids(isolated_database, source_csv):
    pipeline.run(source_csv)
    frame = pd.read_csv(source_csv)
    existing_id = frame.loc[0, "Loan_ID"]
    updated_income = int(frame.loc[0, "ApplicantIncome"]) + 1000
    frame.loc[0, "ApplicantIncome"] = updated_income
    new_row = frame.iloc[[1]].copy()
    new_row["Loan_ID"] = "LP_TEST_UPSERT"
    pd.concat([frame, new_row], ignore_index=True).to_csv(source_csv, index=False)

    pipeline.run(source_csv, load_mode="upsert")
    upsert_run = latest_run(isolated_database)
    assert upsert_run["status"] == "SUCCESS"
    assert upsert_run["bronze_rows"] == upsert_run["silver_rows"] == upsert_run["gold_rows"] == 49
    with isolated_database.connect() as connection:
        assert connection.execute(text("""
            SELECT "ApplicantIncome" FROM bronze.loan_applications WHERE "Loan_ID" = :loan_id
        """), {"loan_id": existing_id}).scalar_one() == updated_income
        assert connection.execute(text("""
            SELECT operation, loan_id FROM control.cdc_events
            WHERE run_id = :run_id ORDER BY operation, loan_id
        """), {"run_id": upsert_run["run_id"]}).all() == [
            ("INSERT", "LP_TEST_UPSERT"),
            ("UPDATE", existing_id),
        ]
