import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medalloan.pipeline import REQUIRED_SOURCE_COLUMNS  # noqa: E402


def test_all_source_partitions_match_regression_contract():
    files = sorted((ROOT / "data").glob("loan_data_*.csv"))
    assert len(files) == 8

    frames = [pd.read_csv(file) for file in files]
    combined = pd.concat(frames, ignore_index=True)

    assert len(combined) == 381
    assert REQUIRED_SOURCE_COLUMNS.issubset(combined.columns)
    assert combined["Loan_ID"].notna().all()
    assert combined["Loan_ID"].astype(str).str.strip().ne("").all()
    assert combined["Loan_ID"].is_unique
    assert set(combined["Loan_Status"].dropna().unique()) <= {"Y", "N"}


def test_source_partition_row_counts_are_stable():
    counts = {
        file.name: len(pd.read_csv(file))
        for file in sorted((ROOT / "data").glob("loan_data_*.csv"))
    }
    assert counts == {
        "loan_data_01.csv": 48,
        "loan_data_02.csv": 48,
        "loan_data_03.csv": 48,
        "loan_data_04.csv": 48,
        "loan_data_05.csv": 48,
        "loan_data_06.csv": 48,
        "loan_data_07.csv": 48,
        "loan_data_08.csv": 45,
    }
