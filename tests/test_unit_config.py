import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from medalloan.config import ConfigurationError, Settings  # noqa: E402


def test_settings_reject_invalid_quality_mode(monkeypatch):
    for key, value in {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_NAME": "medalloan",
        "DB_USER": "postgres",
        "DB_PASSWORD": "secret",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("QUALITY_FAILURE_MODE", "INVALID")

    with pytest.raises(ConfigurationError, match="QUALITY_FAILURE_MODE"):
        Settings.from_env()
