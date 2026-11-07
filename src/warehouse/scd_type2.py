"""Maintain customer history with Slowly Changing Dimension Type 2 semantics."""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.loading.watermarks import advance_pipeline_watermark
from src.utils.database import create_database_engine

logger = logging.getLogger(__name__)
DEFAULT_CUSTOMER_PIPELINE = "dim_customer_scd2"
TRACKED_COLUMNS = ("customer_name", "email", "city", "state")
REQUIRED_COLUMNS = ("customer_id", *TRACKED_COLUMNS, "updated_at")

SELECT_CURRENT_CUSTOMER = text(
    """
        SELECT customer_key, customer_name, email, city, state,
            effective_start_date, source_updated_at
    FROM dim_customer
    WHERE customer_id = :customer_id AND is_current = TRUE
    FOR UPDATE
    """
)
EXPIRE_CUSTOMER_VERSION = text(
    """
    UPDATE dim_customer
    SET effective_end_date = :effective_end_date, is_current = FALSE
    WHERE customer_key = :customer_key AND is_current = TRUE
    """
)
INSERT_CUSTOMER_VERSION = text(
    """
    INSERT INTO dim_customer (
        customer_id, customer_name, email, city, state,
        effective_start_date, effective_end_date, is_current, source_updated_at
    )
    VALUES (
        :customer_id, :customer_name, :email, :city, :state,
        :effective_start_date, NULL, TRUE, :source_updated_at
    )
    """
)
TOUCH_CUSTOMER_SOURCE_TIMESTAMP = text(
    """
    UPDATE dim_customer
    SET source_updated_at = :source_updated_at
    WHERE customer_key = :customer_key AND is_current = TRUE
    """
)


class CustomerDimensionError(RuntimeError):
    """Raised when customer history cannot be updated consistently."""


@dataclass
class SCD2LoadResult:
    """Counts of customer rows inserted, changed, unchanged, or ignored as stale."""

    inserted: int = 0
    changed: int = 0
    unchanged: int = 0
    stale: int = 0
    watermark_at: datetime | None = None


def _effective_date(value: Any) -> date:
    parsed_date = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed_date):
        raise ValueError(f"Invalid customer updated_at value: {value!r}")
    return parsed_date.date()


def _normalized_value(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return str(value).strip()


def apply_customer_changes(
    customers: pd.DataFrame,
    *,
    pipeline_name: str = DEFAULT_CUSTOMER_PIPELINE,
    engine: Engine | None = None,
) -> SCD2LoadResult:
    """Insert new customer versions and expire changed current versions atomically.

    `effective_end_date` is exclusive. A source change with an earlier effective date
    than the current version is treated as stale and does not replace newer history.
    """
    if not isinstance(customers, pd.DataFrame):
        raise TypeError("customers must be a pandas DataFrame.")
    if customers.empty:
        return SCD2LoadResult()

    missing_columns = sorted(set(REQUIRED_COLUMNS) - set(customers.columns))
    if missing_columns:
        raise ValueError(f"Customer data is missing columns: {', '.join(missing_columns)}")

    input_rows = customers.loc[:, REQUIRED_COLUMNS].copy()
    input_rows["customer_id"] = input_rows["customer_id"].map(_normalized_value)
    if input_rows["customer_id"].isna().any():
        raise ValueError("customer_id cannot be null or blank for SCD Type 2 loading.")

    for column in TRACKED_COLUMNS:
        input_rows[column] = input_rows[column].map(_normalized_value)
        if input_rows[column].isna().any():
            raise ValueError(f"{column} cannot be null or blank for SCD Type 2 loading.")

    source_timestamps = pd.to_datetime(input_rows["updated_at"], errors="coerce", utc=True)
    if source_timestamps.isna().any():
        raise ValueError("Customer records contain invalid updated_at timestamps.")
    input_rows["_source_updated_at"] = source_timestamps
    input_rows["_effective_start_date"] = source_timestamps.dt.date
    input_rows = input_rows.sort_values(
        ["customer_id", "_source_updated_at"], kind="mergesort"
    )
    latest_update = input_rows["_source_updated_at"].max().to_pydatetime()

    owns_engine = engine is None
    active_engine = engine if engine is not None else create_database_engine()
    result = SCD2LoadResult(watermark_at=latest_update)
    try:
        with active_engine.begin() as connection:
            for record in input_rows.to_dict(orient="records"):
                customer_id = record["customer_id"]
                effective_start_date = record["_effective_start_date"]
                current_result = connection.execute(
                    SELECT_CURRENT_CUSTOMER, {"customer_id": customer_id}
                )
                current = current_result.mappings().one_or_none()

                tracked_values = {
                    column: record[column] for column in TRACKED_COLUMNS
                }
                if current is None:
                    connection.execute(
                        INSERT_CUSTOMER_VERSION,
                        {
                            "customer_id": customer_id,
                            **tracked_values,
                            "effective_start_date": effective_start_date,
                            "source_updated_at": record[
                                "_source_updated_at"
                            ].to_pydatetime(),
                        },
                    )
                    result.inserted += 1
                    continue

                current_start = current["effective_start_date"]
                if isinstance(current_start, datetime):
                    current_start = current_start.date()
                current_source_update = pd.to_datetime(
                    current["source_updated_at"], errors="coerce", utc=True
                )
                incoming_source_update = record["_source_updated_at"]
                values_unchanged = all(
                    _normalized_value(current[column]) == tracked_values[column]
                    for column in TRACKED_COLUMNS
                )

                if pd.isna(current_source_update) or incoming_source_update < current_source_update:
                    if values_unchanged:
                        result.unchanged += 1
                    else:
                        result.stale += 1
                    continue
                if effective_start_date < current_start:
                    result.stale += 1
                    continue

                if values_unchanged:
                    if incoming_source_update > current_source_update:
                        connection.execute(
                            TOUCH_CUSTOMER_SOURCE_TIMESTAMP,
                            {
                                "customer_key": current["customer_key"],
                                "source_updated_at": incoming_source_update.to_pydatetime(),
                            },
                        )
                    result.unchanged += 1
                    continue

                expiration_result = connection.execute(
                    EXPIRE_CUSTOMER_VERSION,
                    {
                        "customer_key": current["customer_key"],
                        "effective_end_date": effective_start_date,
                    },
                )
                if expiration_result.rowcount != 1:
                    raise CustomerDimensionError(
                        f"Expected one current version for customer {customer_id}, "
                        f"expired {expiration_result.rowcount}."
                    )
                connection.execute(
                    INSERT_CUSTOMER_VERSION,
                    {
                        "customer_id": customer_id,
                        **tracked_values,
                        "effective_start_date": effective_start_date,
                        "source_updated_at": incoming_source_update.to_pydatetime(),
                    },
                )
                result.changed += 1
            advance_pipeline_watermark(connection, pipeline_name, latest_update)
    except SQLAlchemyError as error:
        logger.exception("Customer SCD Type 2 transaction failed")
        raise CustomerDimensionError(
            "Could not update customer history; the transaction was rolled back."
        ) from error
    finally:
        if owns_engine:
            active_engine.dispose()

    logger.info(
        "Customer SCD Type 2 load: %s inserted, %s changed, %s unchanged, %s stale",
        result.inserted,
        result.changed,
        result.unchanged,
        result.stale,
    )
    return result