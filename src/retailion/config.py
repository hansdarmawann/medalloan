from dataclasses import dataclass
import os

from dotenv import load_dotenv


class ConfigurationError(ValueError):
    """Raised when runtime configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    quality_failure_mode: str
    quality_rule_version: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        values = {key: os.getenv(key) for key in (
            "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"
        )}
        missing = [key for key, value in values.items() if not value]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")
        try:
            db_port = int(values["DB_PORT"])
        except ValueError as error:
            raise ConfigurationError("DB_PORT must be an integer") from error
        quality_failure_mode = os.getenv("QUALITY_FAILURE_MODE", "STOP").upper()
        if quality_failure_mode not in {"STOP", "WARN", "QUARANTINE"}:
            raise ConfigurationError(
                "QUALITY_FAILURE_MODE must be STOP, WARN, or QUARANTINE"
            )
        return cls(
            db_host=values["DB_HOST"],
            db_port=db_port,
            db_name=values["DB_NAME"],
            db_user=values["DB_USER"],
            db_password=values["DB_PASSWORD"],
            quality_failure_mode=quality_failure_mode,
            quality_rule_version=os.getenv("QUALITY_RULE_VERSION", "1.0.0"),
        )

