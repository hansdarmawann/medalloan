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
CSV -> Bronze -> Silver -> Gold -> Control / Quarantine / BI
```

The pipeline provides `full`, `append`, `upsert`, and `snapshot` ingestion; contract validation, `Loan_ID` deduplication, snapshot-diff CDC, quality checks, operational metrics, and a DAG runner with retries, timeouts, and resource pools. Gold contains applicant-profile, property-area, and loan-status dimensions, an application fact table, and an approval summary.

## Repository structure

```text
data/archived/loan_data.csv     # Archived source dataset
scripts/create_database.py      # Create the database if missing
scripts/run_pipeline.py         # Run the pipeline
scripts/run_orchestrator.py     # Run the lightweight DAG
src/retailion/                  # Pipeline and configuration code
tests/                          # Contract and orchestrator tests
```

## How to run

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

Run the unit tests:

```cmd
python -m pytest tests -p no:cacheprovider -q
```

Expected output:

```text
4 passed
```

The tests verify the loan dataset contract and orchestrator retry behavior. The full pipeline test requires a running PostgreSQL database.

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
