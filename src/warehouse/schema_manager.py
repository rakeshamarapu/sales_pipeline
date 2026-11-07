"""Initialize the warehouse schema and populate the date dimension."""

import logging
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils.database import create_database_engine

logger = logging.getLogger(__name__)
DEFAULT_SCHEMA_PATH = Path(__file__).with_name("star_schema.sql")

POPULATE_DATES_SQL = text(
    """
    INSERT INTO dim_date (date_key, full_date, day, month, quarter, year)
    SELECT
        TO_CHAR(calendar_date, 'YYYYMMDD')::INTEGER,
        calendar_date::DATE,
        EXTRACT(DAY FROM calendar_date)::SMALLINT,
        EXTRACT(MONTH FROM calendar_date)::SMALLINT,
        EXTRACT(QUARTER FROM calendar_date)::SMALLINT,
        EXTRACT(YEAR FROM calendar_date)::SMALLINT
    FROM GENERATE_SERIES(
        CAST(:start_date AS DATE),
        CAST(:end_date AS DATE),
        INTERVAL '1 day'
    ) AS calendar_date
    ON CONFLICT (full_date) DO NOTHING
    """
)


class WarehouseSchemaError(RuntimeError):
    """Raised when PostgreSQL cannot apply the warehouse schema."""


def initialize_star_schema(
    engine: Engine | None = None,
    schema_path: str | Path = DEFAULT_SCHEMA_PATH,
) -> None:
    """Apply the idempotent star-schema DDL in a single transaction."""
    schema_file = Path(schema_path)
    try:
        schema_sql = schema_file.read_text(encoding="utf-8")
    except OSError as error:
        raise FileNotFoundError(f"Could not read warehouse schema file: {schema_file}") from error
    if not schema_sql.strip():
        raise ValueError(f"Warehouse schema file is empty: {schema_file}")

    owns_engine = engine is None
    active_engine = engine if engine is not None else create_database_engine()
    try:
        with active_engine.begin() as connection:
            connection.exec_driver_sql(schema_sql)
    except SQLAlchemyError as error:
        logger.exception("Could not initialize the PostgreSQL star schema")
        raise WarehouseSchemaError(
            "Could not initialize the PostgreSQL star schema. "
            "Check database connectivity and the schema error details."
        ) from error
    finally:
        if owns_engine:
            active_engine.dispose()

    logger.info("Initialized PostgreSQL star schema from %s", schema_file)


def populate_date_dimension(
    start_date: date,
    end_date: date,
    engine: Engine | None = None,
) -> int:
    """Insert each date in the inclusive range, leaving existing dates unchanged."""
    if not isinstance(start_date, date) or isinstance(start_date, datetime):
        raise TypeError("start_date must be a datetime.date value.")
    if not isinstance(end_date, date) or isinstance(end_date, datetime):
        raise TypeError("end_date must be a datetime.date value.")
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date.")

    owns_engine = engine is None
    active_engine = engine if engine is not None else create_database_engine()
    try:
        with active_engine.begin() as connection:
            result = connection.execute(
                POPULATE_DATES_SQL,
                {"start_date": start_date, "end_date": end_date},
            )
            inserted_count = result.rowcount
    except SQLAlchemyError as error:
        logger.exception("Could not populate dim_date")
        raise WarehouseSchemaError(
            "Could not populate dim_date. Initialize the warehouse schema first."
        ) from error
    finally:
        if owns_engine:
            active_engine.dispose()

    return max(inserted_count, 0)