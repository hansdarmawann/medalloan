"""Create the configured PostgreSQL database when it does not exist."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import psycopg2  # noqa: E402
from psycopg2 import sql  # noqa: E402

from retailion.config import Settings  # noqa: E402


def main() -> None:
    settings = Settings.from_env()
    connection = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname="postgres",
        user=settings.db_user,
        password=settings.db_password,
    )
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (settings.db_name,),
            )
            exists = cursor.fetchone() is not None
            if exists:
                print(f"Database already exists: {settings.db_name}")
                return

            cursor.execute(sql.SQL("CREATE DATABASE {} ").format(sql.Identifier(settings.db_name)))
            print(f"Database created: {settings.db_name}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()

