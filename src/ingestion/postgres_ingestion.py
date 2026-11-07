"""Extract existing retail records from PostgreSQL into raw snapshots."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

from src.ingestion.common import add_ingestion_metadata, default_raw_directory, write_raw_snapshot
from src.utils.database import DatabaseConfigurationError, create_database_engine

logger = logging.getLogger(__name__)
ALLOWED_TABLES = frozenset({"customers", "products", "orders"})


class PostgresExtractionError(RuntimeError):
    """Raised when a PostgreSQL table cannot be extracted."""


def extract_table(
    table_name: str,
    *,
    engine: Engine | None = None,
    raw_directory: str | Path | None = None,
) -> pd.DataFrame:
    """Extract an allowed source table, annotate its rows, and save a raw snapshot."""
    table = table_name.strip().lower()
    if table not in ALLOWED_TABLES:
        raise ValueError(
            f"Unsupported table '{table_name}'. Allowed tables: {', '.join(sorted(ALLOWED_TABLES))}."
        )

    owns_engine = engine is None
    active_engine = engine
    try:
        if active_engine is None:
            active_engine = create_database_engine()
        with active_engine.connect() as connection:
            records = pd.read_sql_query(text(f"SELECT * FROM {table}"), connection)
    except (SQLAlchemyError, DatabaseConfigurationError) as error:
        logger.exception("PostgreSQL extraction failed for table %s", table)
        raise PostgresExtractionError(
            f"PostgreSQL extraction failed for table '{table}'. Check the database settings and connection."
        ) from error
    finally:
        if owns_engine and active_engine is not None:
            active_engine.dispose()

    timestamp = datetime.now(timezone.utc)
    enriched = add_ingestion_metadata(records, "postgres", timestamp)
    write_raw_snapshot(
        enriched,
        table,
        "postgres",
        timestamp,
        raw_directory or default_raw_directory(),
    )
    logger.info("Extracted %s records from PostgreSQL table %s", len(enriched), table)
    return enriched


def extract_retail_tables(
    *,
    engine: Engine | None = None,
    raw_directory: str | Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Extract customers, products, and orders using one database connection pool."""
    owned_engine = engine or create_database_engine()
    try:
        return {
            table: extract_table(
                table,
                engine=owned_engine,
                raw_directory=raw_directory,
            )
            for table in ("customers", "products", "orders")
        }
    finally:
        if engine is None:
            owned_engine.dispose()