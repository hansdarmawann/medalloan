import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, URL

from .config import Settings


def ensure_database_exists(settings: Settings) -> None:
    """Create the configured database through PostgreSQL's maintenance database."""
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
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE DATABASE {} ").format(sql.Identifier(settings.db_name)))
    finally:
        connection.close()


def create_db_engine(settings: Settings):
    """Create an engine safely, including passwords with special characters."""
    url = URL.create(
        "postgresql+psycopg2",
        username=settings.db_user,
        password=settings.db_password,
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
    )
    return create_engine(
        url,
        future=True,
        pool_pre_ping=True,
    )
