import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from retailion.pipeline import REQUIRED_SOURCE_COLUMNS, validate_source_schema


def test_source_contract_accepts_required_columns():
    validate_source_schema(REQUIRED_SOURCE_COLUMNS)


def test_source_contract_rejects_missing_columns():
    with pytest.raises(ValueError, match="missing columns"):
        validate_source_schema(REQUIRED_SOURCE_COLUMNS - {"Profit"})
