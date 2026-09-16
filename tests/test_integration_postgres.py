import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import text

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "src"))

from medalloan.config import Settings  # noqa: E402
from medalloan.database import create_db_engine  # noqa: E402


pytestmark = pytest.mark.integration


def test_postgres_runtime_has_expected_pipeline_schemas():
    if os.getenv("RUN_POSTGRES_INTEGRATION") != "1":
        pytest.skip("Set RUN_POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")

    engine = create_db_engine(Settings.from_env())
    try:
        with engine.connect() as connection:
            dialect = connection.execute(text("SELECT current_database()"))
            assert dialect.scalar_one() == Settings.from_env().db_name
            assert connection.dialect.name == "postgresql"
    finally:
        engine.dispose()
