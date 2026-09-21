# Medalloan PostgreSQL Data Warehouse

Medalloan is a learning-focused data engineering project for loading CSV data into PostgreSQL using the medallion pattern: Bronze, Silver, and Gold, with quality controls, operational metadata, and lightweight orchestration.

## Dataset

The available dataset is [`data/archived/loan_data.csv`](data/archived/loan_data.csv), containing **381 rows and 13 columns** of loan application data:

```text
Loan_ID, Gender, Married, Dependents, Education, Self_Employed,
ApplicantIncome, CoapplicantIncome, LoanAmount, Loan_Amount_Term,
Credit_History, Property_Area, Loan_Status
```

Numeric columns include `ApplicantIncome`, `CoapplicantIncome`, `LoanAmount`, and `Loan_Amount_Term`. `Credit_History` is numeric/binary, while `Loan_Status` contains the approval target (`Y`/`N`). Missing values occur in `Gender` (5), `Dependents` (8), `Self_Employed` (21), `Loan_Amount_Term` (11), and `Credit_History` (30).

The eight dataset parts are stored in `data/` as `loan_data_01.csv` through `loan_data_08.csv`. The pipeline reads all of them automatically; the original `data/archived/loan_data.csv` file is retained as an archive.

## Current architecture

```text
CSV -> Bronze -> Silver -> Gold candidate -> Quality gate -> Gold / BI
                            |                    |
                       Run staging        Control / quality audit
```

The pipeline provides `full`, `append`, `upsert`, and `snapshot` ingestion; contract validation, `Loan_ID` deduplication, snapshot-diff CDC, quality checks, operational metrics, and a DAG runner with retries, timeouts, and resource pools. Gold contains applicant-profile, property-area, and loan-status dimensions, an application fact table, and an approval summary.

### Ingestion modes

- `full` replaces the current Bronze table with the incoming dataset.
- `append` inserts only previously unseen `Loan_ID` values. Existing rows are
  left unchanged, even when the incoming values differ, so rerunning the same
  batch is idempotent.
- `upsert` inserts new IDs and replaces existing rows with their incoming
  values.
- `snapshot` stores the incoming dataset in snapshot history and also replaces
  the current Bronze table.

CDC records only applied operations: append runs emit `INSERT` events for new
IDs and no `UPDATE` events, while upsert runs emit both inserts and changed
updates. Full and snapshot runs also compare the prior current Bronze state and
emit `DELETE` events for IDs absent from the incoming complete snapshot. Those
rows are removed from current Bronze, Silver, and Gold on the same run. The
`bronze_rows` metric represents the resulting Bronze table size, which keeps it
comparable with Silver and Gold row counts.

### Gold publication and rollback

Each run builds candidate Gold tables in a unique `gold_stage_<run_id>` schema.
The four quality checks (Silver/Gold row counts, unique loan IDs, valid loan
measures, and valid loan status) run against that candidate before publication.
With `QUALITY_FAILURE_MODE=STOP` (the default), a failed check rejects the
candidate and preserves every previously published Gold table. On a failed
first run, no Gold tables are published. Bronze and Silver retain the latest
input and transformation for investigation.

Gold table replacement, the `SUCCESS` status, and success metrics commit in one
PostgreSQL transaction. A publication error rolls them all back. Quality results
are committed separately to `control.data_quality_results`, so they survive a
rollback; `FAILED` is recorded after rollback. Staging schemas are removed on
success or rolled back on failure. A passing quality audit alone does not prove
publication succeeded; also check `control.pipeline_runs.status`.

The gate uses `QUALITY_FAILURE_MODE` and `QUALITY_RULE_VERSION` from runtime
settings, including `.env`. `WARN` and `QUARANTINE` currently both log failed
checks and allow publication; invalid rows are still copied to quarantine by
the Silver step, without being excluded from Gold. This change does not add
row filtering or new NULL rules.

After the first publication, Silver and Gold keep the same table objects and
refresh their rows with transactional `TRUNCATE` and `INSERT`. This preserves
object IDs, views, grants, indexes, and constraints. External foreign keys can
still block a truncate; the pipeline does not use `CASCADE`, and rolls back
instead of removing those dependencies. A failed refresh restores the previous
rows.

### Concurrent-run protection

Bronze, Silver, and Gold are shared tables, so one PostgreSQL database accepts
one Medalloan pipeline run at a time. Each run obtains a database advisory lock
before it creates control tables or changes warehouse data. A second run fails
immediately with a retryable `PipelineError`; it does not create a run record or
change any layer. The lock is released when the active run completes, fails, or
its database session closes.

Bronze staging tables are named `loan_applications_stage_<run_id>` and removed
after use. This prevents one run from reusing or deleting another run's staging
data, while the advisory lock preserves a consistent view across Bronze, Silver,
quality validation, and Gold publication.

## Repository structure

```text
data/archived/loan_data.csv     # Archived source dataset
Dockerfile                      # Reproducible Python application image
compose.yaml                    # PostgreSQL, pipeline, and integration-test services
requirements-runtime.txt        # Minimal pipeline and container-test dependencies
scripts/create_database.py      # Create the database if missing
scripts/run_pipeline.py         # Run the pipeline
scripts/run_orchestrator.py     # Run the lightweight DAG
migrations/                     # Ordered, checksum-tracked SQL migrations
src/medalloan/                  # Pipeline and configuration code
tests/                          # Contract and orchestrator tests
```

## Run with Docker

Docker Compose provides PostgreSQL 16, the pipeline application, database
health checks, persistent database storage, and an opt-in integration-test
service. Docker Desktop must be running in Linux-container mode.

Create the local environment file and change `DB_PASSWORD` before using the
stack outside an isolated development machine:

```cmd
copy .env.example .env
```

Build the application image, start PostgreSQL, run the pipeline, and stop the
stack when the pipeline exits:

```cmd
docker compose up --build --abort-on-container-exit --exit-code-from pipeline
```

PostgreSQL data remains in the `postgres_data` named volume. For repeated runs,
the services can be managed independently:

```cmd
docker compose up -d postgres
docker compose run --rm pipeline
```

Run the DAG orchestrator with the same image and database:

```cmd
docker compose run --rm pipeline python scripts/run_orchestrator.py --source /app/data
```

Run the complete test suite, including tests that create disposable PostgreSQL
databases:

```cmd
docker compose --profile test run --build --rm tests
```

Stop the stack without deleting its database:

```cmd
docker compose down
```

To intentionally delete all local PostgreSQL data and start from an empty
database, remove the named volume as well:

```cmd
docker compose down --volumes
```

PostgreSQL is published to `127.0.0.1:5434` by default so it does not clash
with a native PostgreSQL installation or the Promptchived database. Application
containers still connect to `postgres:5432`. Override `POSTGRES_HOST_PORT` in
`.env` when another host port is preferred. To open an interactive database
shell, run:

```cmd
docker compose exec postgres psql -U postgres -d medalloan
```

## Run without Docker

### Prerequisites

Install Anaconda or Miniconda, and make sure PostgreSQL is running. The PostgreSQL user must be allowed to create databases, schemas, tables, views, indexes, and the `pgcrypto` extension.

The commands below use Windows CMD.

### 1. Create the Conda environment

Run these commands from the repository root:

```cmd
conda create -n medalloan python=3.10 -y
conda activate medalloan
python -m pip install -r requirements.txt
```

Check that the environment and dependencies are available:

```cmd
conda env list
python --version
python -m pip show pandas sqlalchemy pytest
```

### 2. Configure the database

Create the environment file:

```cmd
copy .env.example .env
```

Open `.env` and set the PostgreSQL values:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=medalloan
DB_USER=postgres
DB_PASSWORD=your-password
```

Create the database if it does not exist:

```cmd
python scripts\create_database.py
```

Apply database migrations explicitly before a deployment or after upgrading
the repository:

```cmd
python scripts\migrate.py
```

Applied versions and SHA-256 checksums are stored in
`control.schema_migrations`. An already applied migration cannot be edited;
the runner stops and requires a new numbered migration. The pipeline acquires
the same database advisory lock and applies pending migrations automatically,
so a normal run remains safe when the explicit command was skipped. The
current migrations initialize the ledger and add serving metadata columns to
older Silver/Gold tables when those tables already exist.

### 3. Run the pipeline

Run the complete pipeline. By default, it reads all eight files in `data/`:

```cmd
python scripts\run_pipeline.py
```

The input files are:

```text
data\loan_data_01.csv
data\loan_data_02.csv
data\loan_data_03.csv
data\loan_data_04.csv
data\loan_data_05.csv
data\loan_data_06.csv
data\loan_data_07.csv
data\loan_data_08.csv
```

To run one CSV file only:

```cmd
python scripts\run_pipeline.py --source data\loan_data_01.csv
```

To view all available command options:

```cmd
python scripts\run_pipeline.py --help
```

### 4. Run the orchestrator

Run the pipeline through the lightweight DAG orchestrator:

```cmd
python scripts\run_orchestrator.py
```

Test retry behavior by forcing the first attempt to fail:

```cmd
python scripts\run_orchestrator.py --inject-failure
```

### 5. Run the tests

Run the tests that do not require PostgreSQL:

```cmd
python -m pytest tests -p no:cacheprovider -q -m "not integration"
```

These cover configuration, the source contract, dataset regression, and retries.
To also run PostgreSQL integration tests:

```cmd
set RUN_POSTGRES_INTEGRATION=1
python -m pytest tests -p no:cacheprovider -q
```

The PostgreSQL user needs `CREATEDB` permission for publication tests. Each test
creates a uniquely named `medalloan_test_<uuid>` database and drops only that
database during teardown; publication tests never write to the configured
application database. They verify valid publication, rejected candidates,
first-run failures, rollback during table replacement and success recording,
durable quality audits, and nonblocking policies. CI runs this suite against
its PostgreSQL service.

### 6. Verify the database output

After a successful pipeline run, connect to PostgreSQL and check the generated tables:

```sql
SELECT status, bronze_rows, silver_rows, gold_rows
FROM control.pipeline_runs
ORDER BY started_at DESC
LIMIT 5;

SELECT COUNT(*) FROM bronze.loan_applications;
SELECT COUNT(*) FROM silver.loan_applications;
SELECT COUNT(*) FROM gold.fact_loan_applications;
SELECT * FROM gold.loan_approval_summary;
```

The expected full-load row count is 381 records.

## Dataset usage notes

The loan dataset is retained as archived data. Review its source and usage terms before redistribution or use beyond learning purposes. This repository does not currently include a software license file.
