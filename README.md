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
CSV sumber -> Bronze -> Silver -> Gold -> Control / Quarantine / BI
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

## Quick start

Prerequisites: Python 3.10+, a running PostgreSQL instance, and an account allowed to create schemas, tables, views, indexes, and the `pgcrypto` extension.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
python scripts/create_database.py
```

Set `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` in `.env`, then run:

```powershell
python scripts/run_pipeline.py
```

This command processes all eight `data/loan_data_*.csv` files. To run only one dataset part, provide its file path explicitly:

```powershell
python scripts/run_pipeline.py --source data/loan_data_01.csv
```

## Testing

```powershell
python -m pip install pytest
python -m pytest -q
```

Tests verify the loan dataset contract and orchestrator retry behavior. Run the pipeline against PostgreSQL to validate the complete Bronze, Silver, and Gold transformations.

## Dataset usage notes

The loan dataset is retained as archived data. Review its source and usage terms before redistribution or use beyond learning purposes. This repository does not currently include a software license file.
