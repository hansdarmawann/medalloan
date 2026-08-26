# Medalloan PostgreSQL Data Warehouse

Medalloan adalah proyek pembelajaran data engineering untuk memuat data CSV ke PostgreSQL dengan pola medallion: Bronze, Silver, dan Gold, disertai kontrol kualitas, metadata operasional, dan orkestrasi sederhana.

## Dataset

Dataset yang tersedia adalah [`data/archived/loan_data.csv`](data/archived/loan_data.csv), berisi **381 baris dan 13 kolom** data pengajuan pinjaman:

```text
Loan_ID, Gender, Married, Dependents, Education, Self_Employed,
ApplicantIncome, CoapplicantIncome, LoanAmount, Loan_Amount_Term,
Credit_History, Property_Area, Loan_Status
```

Kolom numerik meliputi `ApplicantIncome`, `CoapplicantIncome`, `LoanAmount`, dan `Loan_Amount_Term`. `Credit_History` bersifat numerik/biner, sedangkan `Loan_Status` berisi target persetujuan (`Y`/`N`). Nilai kosong terdapat pada `Gender` (5), `Dependents` (8), `Self_Employed` (21), `Loan_Amount_Term` (11), dan `Credit_History` (30).

File berada di folder `archived` dan **belum menjadi input default pipeline**. Pipeline masih mengimplementasikan kontrak Superstore berikut:

```text
Row ID, Order ID, Order Date, Ship Date, Customer ID,
Product ID, Sales, Quantity, Discount, Profit
```

Karena itu, `loan_data.csv` tidak dapat langsung dijalankan tanpa perubahan kontrak, transformasi, dan model Gold.

## Arsitektur saat ini

```text
CSV sumber -> Bronze -> Silver -> Gold -> Control / Quarantine / BI
```

Pipeline menyediakan ingestion `full`, `append`, `upsert`, dan `snapshot`; validasi kontrak, deduplikasi, watermark, replay, snapshot-diff CDC; dimensional modeling Superstore; quality score, SLA, lineage, audit, governance; serta DAG runner dengan retry, timeout, dan resource pool.

## Struktur repository

```text
data/archived/loan_data.csv     # Dataset pinjaman terbaru
scripts/create_database.py      # Membuat database jika belum ada
scripts/run_pipeline.py         # Menjalankan pipeline
scripts/run_orchestrator.py     # Menjalankan DAG sederhana
src/retailion/                  # Kode pipeline dan konfigurasi
tests/                          # Test kontrak dan orchestrator
```

## Quick start

Prasyarat: Python 3.10+, PostgreSQL yang berjalan, dan akun dengan izin membuat schema, tabel, view, index, serta extension `pgcrypto`.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
python scripts/create_database.py
```

Isi `.env` dengan `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, dan `DB_PASSWORD`, lalu jalankan:

```powershell
python scripts/run_pipeline.py
```

Perintah tersebut masih menggunakan default input Superstore yang dirujuk kode. Dataset pinjaman belum kompatibel dan perintah berikut akan gagal pada validasi kontrak:

```powershell
python scripts/run_pipeline.py --source data/archived/loan_data.csv
```

## Pengembangan dataset pinjaman

Untuk menjadikan dataset baru sebagai sumber utama, perlu dilakukan: mengganti `REQUIRED_SOURCE_COLUMNS`, menangani null dan tipe data, membangun Silver dengan grain `Loan_ID`, mendesain fact/dimensi pinjaman, memperbarui aturan kualitas, lineage, query, default path script, dan test kontraknya.

## Pengujian

```powershell
python -m pip install pytest
python -m pytest -q
```

Test saat ini memverifikasi kontrak Superstore dan perilaku retry orchestrator; dataset pinjaman belum diuji oleh pipeline.

## Catatan penggunaan dataset

Dataset pinjaman disimpan sebagai data arsip. Periksa sumber dan ketentuan penggunaannya sebelum redistribusi atau pemakaian di luar pembelajaran. Repository ini belum menyertakan file lisensi perangkat lunak.
