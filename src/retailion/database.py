from sqlalchemy import create_engine, URL

from .config import Settings


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
