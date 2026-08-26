# Retailion PostgreSQL Data Warehouse

Retailion is a learning-focused data engineering project that turns a retail sales CSV into an analytics-ready PostgreSQL warehouse.

It demonstrates a complete batch pipeline using the medallion pattern:

```text
Sample - Superstore.csv
          |
          v
Bronze: raw source data
          |
          v
Silver: cleaned and typed data
          |
          v
Gold: dimensions, facts, and aggregates
          |
          v
SQL analysis / BI tools / dashboards
```

The main workflow is a Python command-line pipeline. Three Jupyter notebooks are also included for people who prefer to learn each warehouse layer interactively.

## Contents

- [What this project includes](#what-this-project-includes)
- [Architecture](#architecture)
- [Project structure](#project-structure)
- [Quick start](#quick-start)
- [Running the pipeline](#running-the-pipeline)
- [Load modes and incremental processing](#load-modes-and-incremental-processing)
- [Warehouse data model](#warehouse-data-model)
- [Data quality and operational metadata](#data-quality-and-operational-metadata)
- [Orchestration](#orchestration)
- [Notebooks](#notebooks)
- [Testing](#testing)
- [Example SQL queries](#example-sql-queries)
- [Troubleshooting](#troubleshooting)
- [Current scope and limitations](#current-scope-and-limitations)

## What this project includes

- A repeatable CSV-to-PostgreSQL ETL pipeline
- Bronze, Silver, Gold, Control, and Quarantine schemas
- Full, append, upsert, and snapshot ingestion modes
- Watermark-based incremental processing with an overlap window
- Explicit date-window replay and backfill support
- Snapshot-diff CDC records for inserts, updates, and deletes
- Source schema validation and file consistency checks
- Dimensional modeling with SCD Type 1 and Type 2 customer dimensions
- A date-partitioned sales fact table
- Daily and monthly semantic serving tables
- Data contracts, reconciliation checks, and quality scores
- Pipeline run history, SLA results, metrics, lineage, and failure evidence
- Basic data classification, masking, encryption, retention, and policy metadata
- A small dependency-free DAG runner with retries and trigger validation
- Unit tests for the source contract and orchestrator retry behavior

The included source file, [`data/Sample - Superstore.csv`](data/Sample%20-%20Superstore.csv), contains **9,994 rows and 21 columns**.

## Architecture

### Bronze layer

The Bronze layer is the raw landing area.

The pipeline:

- Reads the CSV with pandas using Latin-1 encoding
- Hashes the file before and after extraction to detect mid-load changes
- Validates required source columns
- Loads through a staging table
- Deduplicates and enforces uniqueness using `Row ID`
- Optionally stores a historical source snapshot
- Compares source and target snapshots to record CDC events

Main objects:

- `bronze.superstore`
- `bronze.superstore_snapshots` when snapshot mode is used

### Silver layer

The Silver layer is the cleaned, standardized dataset at one row per source `row_id`.

Transformations include:

- Column names converted to `snake_case`
- Dates parsed from `MM/DD/YYYY`
- Numeric values explicitly cast
- Customer names trimmed
- Missing postal codes replaced with `00000`
- Rows without an `Order ID` excluded
- Pipeline metadata added: `run_id`, `source_file`, `ingested_at`, and `pipeline_version`
- Invalid business measures copied to the Quarantine schema

Main objects:

- `silver.superstore`
- `quarantine.superstore_invalid`

### Gold layer

The Gold layer is rebuilt from Silver for analytics and reporting.

It contains:

- Customer, product, location, and date dimensions
- A persistent SCD Type 2 customer history table
- A transaction-level, yearly partitioned sales fact
- An order-fulfillment accumulating snapshot
- Daily and monthly aggregate tables
- A masked customer view for safer analytics access

### Control plane

The `control` schema stores operational evidence rather than business facts. It includes run history, watermarks, quality results, CDC events, source schema history, SLA measurements, metrics, lineage, audit events, and incident information.

## Project structure

```text
retailion-pgdb/
|-- data/
|   `-- Sample - Superstore.csv     # Bundled source dataset
|-- notebooks/
|   |-- 01_bronze.ipynb            # Raw ingestion walkthrough
|   |-- 02_silver.ipynb            # Cleaning, quality, and EDA
|   `-- 03_gold.ipynb              # Dimensional modeling and analysis
|-- scripts/
|   |-- create_database.py         # Creates DB_NAME when it is missing
|   |-- run_pipeline.py            # Main production-style CLI
|   `-- run_orchestrator.py        # Lightweight DAG wrapper
|-- src/retailion/
|   |-- config.py                  # Environment configuration
|   |-- database.py                # SQLAlchemy engine factory
|   |-- orchestrator.py            # DAG and Task primitives
|   `-- pipeline.py                # Bronze, Silver, Gold, and controls
|-- tests/
|   |-- test_orchestrator.py
|   `-- test_pipeline_contract.py
|-- .env.example                   # Configuration template
|-- requirements.txt               # Runtime dependencies
`-- README.md
```

## Quick start

### 1. Prerequisites

You need:

- Python **3.10 or newer**
- A running PostgreSQL server
- A PostgreSQL login that can create schemas, tables, views, indexes, and the `pgcrypto` extension
- Permission to create a database if you use `scripts/create_database.py`

PostgreSQL and Python are not installed by this repository. Docker configuration is not included.

### 2. Create and activate a virtual environment

From the repository root:

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS or Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 4. Create your environment file

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS or Linux:

```bash
cp .env.example .env
```

Open `.env` and set your PostgreSQL connection values, for example:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retailion
DB_USER=postgres
DB_PASSWORD=your-password
```

Do not commit `.env`; it is already ignored by Git.

### 5. Create the database

If `DB_NAME` does not exist, run:

```bash
python scripts/create_database.py
```

The script connects to PostgreSQL's `postgres` maintenance database, checks whether `DB_NAME` exists, and creates it only when necessary. It does not delete or replace an existing database.

If your login cannot create databases, ask a database administrator to create it or run an equivalent command with an authorized account:

```sql
CREATE DATABASE retailion;
```

### 6. Run the pipeline

```bash
python scripts/run_pipeline.py
```

The default command uses the bundled CSV and `full` mode. A successful run logs its run ID and Bronze, Silver, and Gold row counts.

### 7. Confirm the result

Using `psql`:

```bash
psql -h localhost -p 5432 -U postgres -d retailion
```

Then run:

```sql
SELECT status, bronze_rows, silver_rows, gold_rows, started_at, finished_at
FROM control.pipeline_runs
ORDER BY started_at DESC
LIMIT 5;

SELECT COUNT(*) FROM bronze.superstore;
SELECT COUNT(*) FROM silver.superstore;
SELECT COUNT(*) FROM gold.fact_sales;
```

## Configuration

The template provides these settings:

| Variable | Default in template | Purpose |
|---|---:|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `retailion` | Target database |
| `DB_USER` | `postgres` | Login role |
| `DB_PASSWORD` | `postgres` | Login password; change this |
| `QUALITY_FAILURE_MODE` | `STOP` | `STOP`, `WARN`, or `QUARANTINE` |
| `QUALITY_RULE_VERSION` | `1.0.0` | Version stored with quality results |
| `PIPELINE_SLA_SECONDS` | `3600` | Expected maximum run duration |
| `DATA_RETENTION_DAYS` | `90` | Retention for selected operational tables |
| `AUDIT_ENCRYPTION_KEY` | placeholder | Key used for the encrypted governance check |

`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` are required. The application raises a configuration error when any is missing or when the port is not an integer.

Important implementation note: quality and SLA constants are currently evaluated when the pipeline module is imported. To override those three values reliably, set them in the process environment before starting Python.

PowerShell example:

```powershell
$env:QUALITY_FAILURE_MODE = 'WARN'
$env:QUALITY_RULE_VERSION = '1.1.0'
$env:PIPELINE_SLA_SECONDS = '900'
python scripts/run_pipeline.py
```

Bash example:

```bash
QUALITY_FAILURE_MODE=WARN QUALITY_RULE_VERSION=1.1.0 PIPELINE_SLA_SECONDS=900 \
  python scripts/run_pipeline.py
```

Use a real secret for `AUDIT_ENCRYPTION_KEY` outside a local demonstration.

## Running the pipeline

### Common commands

Full load from the bundled file:

```bash
python scripts/run_pipeline.py --mode full
```

Use another CSV with the same source contract:

```bash
python scripts/run_pipeline.py --source path/to/superstore.csv
```

Incremental-style upsert with a three-day late-arrival overlap:

```bash
python scripts/run_pipeline.py --mode upsert --overlap-days 3
```

Replay a specific order-date window without advancing the watermark:

```bash
python scripts/run_pipeline.py \
  --mode upsert \
  --start-date 2017-01-01 \
  --end-date 2017-03-31 \
  --replay
```

PowerShell accepts the same command on one line:

```powershell
python scripts/run_pipeline.py --mode upsert --start-date 2017-01-01 --end-date 2017-03-31 --replay
```

Read the CSV in controlled chunks:

```bash
python scripts/run_pipeline.py --mode append --chunk-size 1000 --throttle-ms 100
```

### Pipeline CLI reference

| Option | Default | Meaning |
|---|---|---|
| `--source PATH` | Bundled CSV | Input file |
| `--mode MODE` | `full` | `full`, `append`, `upsert`, or `snapshot` |
| `--start-date YYYY-MM-DD` | None | Inclusive order-date lower bound |
| `--end-date YYYY-MM-DD` | None | Inclusive order-date upper bound |
| `--replay` | Off | Bypass automatic watermark behavior and do not advance it |
| `--overlap-days N` | `2` | Lookback from the current watermark |
| `--chunk-size N` | None | pandas rows per extraction chunk |
| `--throttle-ms N` | `0` | Delay between chunks in milliseconds |

Run `python scripts/run_pipeline.py --help` for the built-in help.

The CLI returns exit code `0` on success and `1` for expected configuration, input, or pipeline failures.

## Load modes and incremental processing

| Mode | Bronze behavior | Typical use |
|---|---|---|
| `full` | Replaces the current Bronze table contents with the staged file | Initial load or complete refresh |
| `append` | Preserves unmatched Bronze rows and replaces rows whose `Row ID` appears in the incoming file | Loading successive extracts |
| `upsert` | Preserves unmatched Bronze rows and replaces rows whose `Row ID` appears in the incoming file | Applying new and changed records |
| `snapshot` | Appends a run-stamped copy to `bronze.superstore_snapshots`, then refreshes current Bronze like `full` | Retaining source snapshots |

In the current implementation, `append` and `upsert` intentionally use the same `Row ID` replacement logic. `append` is therefore idempotent rather than a blind insert-only operation.

### Watermarks

For `append` and `upsert` runs, the pipeline reads `control.pipeline_watermarks` when:

- `--replay` is not set, and
- `--start-date` was not provided.

The calculated lower bound is:

```text
last successful watermark - overlap days
```

The overlap helps reprocess late-arriving records. A watermark advances only after a successful validated publish. If no eligible rows exist, the run can finish with status `NOOP` and preserve the existing Gold data.

Because the source only contains calendar dates, watermarks are stored as UTC-midnight `TIMESTAMPTZ` values.

### Replay and backfill

`--replay` disables automatic watermark selection and prevents watermark advancement. For a safe, predictable backfill, provide both `--start-date` and `--end-date`.

Date windows filter `full`, `snapshot`, and replayed Silver builds. In normal `append` and `upsert` processing, Silver is rebuilt from all retained Bronze rows so historical Gold facts are preserved.

### CDC behavior

The input is a CSV, not a database transaction log. CDC is therefore inferred by comparing the incoming staged snapshot with current Bronze data.

Events are recorded in `control.cdc_events` as:

- `INSERT`: incoming `Row ID` does not exist in Bronze
- `UPDATE`: the same `Row ID` exists but row content differs
- `DELETE`: a Bronze `Row ID` does not exist in the incoming file

Detected deletions are also recorded in `control.source_deletions`. This is snapshot-diff CDC and should not be confused with log-based, real-time CDC.

## Warehouse data model

### Core relationships

```text
gold.dim_customers -----------+
gold.dim_customers_scd2 ------|
gold.dim_products ------------+--> gold.fact_sales
gold.dim_location ------------|
gold.dim_date ----------------+
                                  |
                                  +--> gold.sales_daily
                                  |         |
                                  |         +--> gold.sales_monthly
                                  |
silver.superstore ----------------+--> gold.fact_order_fulfillment
```

### Gold objects

| Object | Grain and purpose |
|---|---|
| `gold.dim_customers` | Latest customer values; SCD Type 1 view of each customer |
| `gold.dim_customers_scd2` | Persistent customer name/segment history with validity timestamps |
| `gold.dim_products` | Latest product values by `product_id` |
| `gold.dim_location` | One row per country, region, state, city, and postal-code combination |
| `gold.dim_date` | One row per order or ship date, with calendar attributes |
| `gold.fact_sales` | One row per source transaction `row_id`; partitioned by order year |
| `gold.fact_order_fulfillment` | One row per order with dates, shipping duration, lines, and totals |
| `gold.sales_daily` | Daily customer/product/location sales aggregate |
| `gold.sales_monthly` | Monthly customer/product/location sales aggregate |
| `gold.dim_customers_masked` | Hashed customer ID and partially masked customer name |

`gold.fact_sales` carries foreign keys to the dimensions and indexes for customer, product, location, and order date. A default partition receives dates not covered by generated yearly partitions.

### Fact measures

The transaction fact exposes the main business measures:

- `sales`
- `quantity`
- `discount`
- `profit`

It also keeps order and ship dates, shipping mode, dimension keys, the source row identifier, the pipeline run ID, and ingestion time.

## Data quality and operational metadata

### Source contract

The CSV must include at least these columns:

```text
Row ID, Order ID, Order Date, Ship Date, Customer ID,
Product ID, Sales, Quantity, Discount, Profit
```

Extra columns are allowed. Missing required columns stop the load before publishing.

### Quality rules

Every publish evaluates four rules:

| Rule | Type | What it checks |
|---|---|---|
| Silver/Gold row count | Technical | Both layers have the same row count |
| Null location keys | Technical | Every fact resolves to a location |
| Invalid measures | Business | `sales >= 0`, `quantity > 0`, and `discount` is between 0 and 1 |
| Silver/Gold totals | Business | Sales, profit, and quantity reconcile within tolerance |

Rows with invalid measures are copied to `quarantine.superstore_invalid` with a reason. Quarantine is currently an evidence copy; it does not remove the row from Silver or Gold.

Failure policies:

- `STOP`: raise an error and mark the run failed
- `WARN`: log a warning and continue
- `QUARANTINE`: log an error indicating quarantine and continue

Results are stored per run in `control.data_quality_results`, `control.data_quality_scores`, and `control.reconciliation_results`.

### Important Control objects

| Category | Objects |
|---|---|
| Run state | `pipeline_runs`, `pipeline_watermarks`, `pipeline_sla_results`, `pipeline_slo_daily` |
| Failures | `pipeline_failures`, `incident_evidence`, `pipeline_monitoring` |
| Quality | `data_contract_rules`, `data_quality_results`, `data_quality_scores`, `reconciliation_results`, `data_profile_results` |
| Source controls | `source_schema_registry`, `source_schema_changes`, `source_deletions`, `cdc_events` |
| Observability | `pipeline_metrics`, `pipeline_traces`, `data_lineage` |
| Governance | `data_classification`, `encrypted_audit_values`, `access_policies`, `audit_events`, `sod_policies` |

### Monitoring queries

Latest pipeline runs:

```sql
SELECT run_id, status, bronze_rows, silver_rows, gold_rows,
       started_at, finished_at, error_message
FROM control.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

Failed-run triage:

```sql
SELECT run_id, failed_what, failed_where, failed_since,
       impact, owner, safe_next_action, error_message
FROM control.pipeline_monitoring
WHERE status = 'FAILED'
ORDER BY started_at DESC;
```

Quality score:

```sql
SELECT q.run_id, q.quality_score, q.quarantined_rows, r.status
FROM control.data_quality_scores q
JOIN control.pipeline_runs r USING (run_id)
ORDER BY q.calculated_at DESC;
```

Recent CDC events:

```sql
SELECT run_id, operation, COUNT(*) AS rows
FROM control.cdc_events
GROUP BY run_id, operation
ORDER BY run_id DESC, operation;
```

### Security and retention controls

After a successful non-NOOP publish, the pipeline:

- Enables PostgreSQL's `pgcrypto` extension
- Creates customer classification metadata
- Creates the masked customer view
- Stores an encrypted governance-check value
- Publishes example access and separation-of-duties policies
- Grants access only if a role named `retailion_bi_reader` already exists
- Removes selected operational evidence older than `DATA_RETENTION_DAYS`

These controls demonstrate patterns; they do not replace your organization's real identity, authorization, key management, backup, or compliance systems.

## Orchestration

The optional orchestrator wraps the pipeline in this task graph:

```text
source_ready -> load_transform_publish -> publish_audit
```

Run it manually:

```bash
python scripts/run_orchestrator.py
```

Run with a configured event trigger:

```bash
python scripts/run_orchestrator.py --trigger source_updated --mode upsert
```

Supported trigger values are:

- `manual`
- `source_updated`
- `backfill_requested`
- The exact value supplied through `--schedule`

The default schedule value, `0 2 * * *`, is metadata only. This repository does not include a scheduler daemon; use cron, Task Scheduler, Airflow, or another external service to invoke the command.

Demonstrate retry recovery by deliberately failing the first load attempt:

```bash
python scripts/run_orchestrator.py --inject-failure
```

The load task allows three attempts with exponential backoff. Tasks also define timeouts and use a named PostgreSQL resource pool. The bundled DAG executes sequentially.

Run `python scripts/run_orchestrator.py --help` for all options. Its source, mode, dates, replay, overlap, chunking, and throttling arguments match the main pipeline.

## Notebooks

The notebooks explain each medallion layer interactively:

1. [`notebooks/01_bronze.ipynb`](notebooks/01_bronze.ipynb) loads and inspects raw data.
2. [`notebooks/02_silver.ipynb`](notebooks/02_silver.ipynb) explores quality, cleaning, and distributions.
3. [`notebooks/03_gold.ipynb`](notebooks/03_gold.ipynb) demonstrates dimensional modeling and analytical queries.

The CLI pipeline is the repeatable operational path; notebooks are best treated as learning and exploration material.

Jupyter itself is not listed in `requirements.txt`. Install it separately if needed:

```bash
python -m pip install jupyterlab
jupyter lab
```

Run Jupyter from the repository root so relative paths resolve consistently.

## Testing

The tests use `pytest`, which is not part of the runtime dependency file. Install and run it with:

```bash
python -m pip install pytest
python -m pytest -q
```

The current suite verifies:

- Acceptance of the required source schema
- Rejection when a required source column is missing
- Recovery after a controlled task failure
- Failure after the retry budget is exhausted

These are unit tests and do not start PostgreSQL or run the complete warehouse pipeline.

## Example SQL queries

### Monthly sales and profit

```sql
SELECT month_key,
       ROUND(SUM(sales)::numeric, 2) AS sales,
       ROUND(SUM(profit)::numeric, 2) AS profit
FROM gold.sales_monthly
GROUP BY month_key
ORDER BY month_key;
```

### Top products by profit

```sql
SELECT p.product_name,
       ROUND(SUM(f.sales)::numeric, 2) AS sales,
       ROUND(SUM(f.profit)::numeric, 2) AS profit
FROM gold.fact_sales f
JOIN gold.dim_products p USING (product_key)
GROUP BY p.product_key, p.product_name
ORDER BY profit DESC
LIMIT 10;
```

### Sales by region

```sql
SELECT l.region,
       ROUND(SUM(f.sales)::numeric, 2) AS sales,
       ROUND(SUM(f.profit)::numeric, 2) AS profit
FROM gold.fact_sales f
JOIN gold.dim_location l USING (location_id)
GROUP BY l.region
ORDER BY sales DESC;
```

### Order fulfillment

```sql
SELECT order_id, order_date, ship_date, days_to_ship,
       line_count, sales, profit
FROM gold.fact_order_fulfillment
ORDER BY days_to_ship DESC, order_id
LIMIT 20;
```

## Troubleshooting

### `Missing environment variables`

Confirm that `.env` exists in the repository root and contains all five required `DB_*` values. Run commands from the repository root.

### `connection refused` or `could not connect to server`

PostgreSQL is probably stopped, listening on a different host or port, or blocked by local networking. Check the PostgreSQL service and compare its connection settings with `.env`.

### `password authentication failed`

Check `DB_USER` and `DB_PASSWORD`. Verify the same credentials directly:

```bash
psql -h localhost -p 5432 -U postgres -d postgres
```

### `database ... does not exist`

Run `python scripts/create_database.py`, or ask an administrator to create the database named by `DB_NAME`.

### `permission denied to create database`

The login needs PostgreSQL's `CREATEDB` privilege for the helper script. Database creation can instead be performed once by an administrator.

### `permission denied to create extension pgcrypto`

The pipeline applies governance controls after publishing and needs permission to run `CREATE EXTENSION IF NOT EXISTS pgcrypto`. Ask an administrator to enable `pgcrypto` in the target database, then rerun the pipeline.

### `ModuleNotFoundError`

Activate the virtual environment and install dependencies again:

```bash
python -m pip install -r requirements.txt
```

### Source schema validation failed

Your CSV is missing one or more required columns, or a header was renamed. Compare its header with the [source contract](#source-contract). Header matching is case-sensitive.

### Date parsing failed

The Silver SQL expects `Order Date` and `Ship Date` in `MM/DD/YYYY` form. Normalize a custom source before loading it.

### A previous run failed

Do not manually advance the watermark. First inspect:

```sql
SELECT *
FROM control.pipeline_monitoring
ORDER BY started_at DESC
LIMIT 5;
```

Correct the source, configuration, permission, or database issue, then rerun. For historical corrections, use a bounded `--replay` window.

## Current scope and limitations

This repository demonstrates important production-style ideas, but it is still a compact educational project.

- PostgreSQL installation, backups, high availability, and disaster recovery are out of scope.
- The CSV is loaded through pandas; PostgreSQL `COPY` or object-storage ingestion would scale better for large files.
- Gold tables are rebuilt on publish; only the SCD Type 2 customer history persists across rebuilds.
- CDC is batch snapshot comparison, not real-time log-based CDC.
- The orchestrator validates schedules and events but does not listen for events or run a clock-based scheduler.
- Quarantined rows are copied for evidence but remain in the published dataset when policy permits continuation.
- The advisory lock is transaction-scoped during run registration; external scheduling should avoid overlapping pipeline processes.
- Example access policies are metadata unless matching PostgreSQL roles are created and administered separately.
- There is no CI workflow, container setup, package build configuration, or integration-test environment in the repository.

## Extending the project

Useful next steps include:

- Add PostgreSQL integration tests in an isolated test database
- Add a Docker Compose development environment
- Package `retailion` with `pyproject.toml`
- Use PostgreSQL `COPY` for larger source files
- Add CI for tests and SQL validation
- Connect a BI tool to the Gold schema
- Add alert delivery for failed runs and missed SLAs
- Replace demonstration keys and policies with managed secrets and real database roles
- Add product or location SCD Type 2 history
- Integrate a production orchestrator when scheduling and distributed execution are required

## Dataset and usage

The bundled file is the commonly used Sample Superstore dataset and was identified in the repository's prior documentation as originating from the [Kaggle Superstore Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final).

No software license file is currently included. Add a license before redistributing or accepting external contributions, and verify the dataset's terms for your intended use.
