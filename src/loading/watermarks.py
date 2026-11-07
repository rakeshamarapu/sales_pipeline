"""Read and atomically advance persisted pipeline watermarks."""

import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from src.ingestion.common import normalize_watermark
from src.utils.database import create_database_engine

logger = logging.getLogger(__name__)

SELECT_WATERMARK = text(
    "SELECT watermark_at FROM pipeline_watermark WHERE pipeline_name = :pipeline_name"
)
UPSERT_WATERMARK = text(
    """
    INSERT INTO pipeline_watermark (pipeline_name, watermark_at)
    VALUES (:pipeline_name, :watermark_at)
    ON CONFLICT (pipeline_name) DO UPDATE
    SET watermark_at = GREATEST(
            pipeline_watermark.watermark_at,
            EXCLUDED.watermark_at
        ),
        updated_at = CURRENT_TIMESTAMP
    """
)


class WatermarkError(RuntimeError):
    """Raised when a pipeline watermark cannot be read or stored."""


def get_pipeline_watermark(
    pipeline_name: str,
    *,
    engine: Engine | None = None,
) -> datetime | None:
    """Return the stored watermark, or None before the first successful load."""
    normalized_name = pipeline_name.strip()
    if not normalized_name:
        raise ValueError("pipeline_name cannot be empty.")

    owns_engine = engine is None
    active_engine = engine if engine is not None else create_database_engine()
    try:
        with active_engine.connect() as connection:
            value = connection.execute(
                SELECT_WATERMARK, {"pipeline_name": normalized_name}
            ).scalar_one_or_none()
    except SQLAlchemyError as error:
        logger.exception("Could not read watermark for %s", normalized_name)
        raise WatermarkError(
            f"Could not read watermark for '{normalized_name}'. Initialize the warehouse schema first."
        ) from error
    finally:
        if owns_engine:
            active_engine.dispose()

    if value is None:
        return None
    return normalize_watermark(value).to_pydatetime()


def advance_pipeline_watermark(
    connection: Connection,
    pipeline_name: str,
    watermark_at: datetime,
) -> None:
    """Advance a pipeline watermark on the caller's active transaction."""
    normalized_name = pipeline_name.strip()
    if not normalized_name:
        raise ValueError("pipeline_name cannot be empty.")
    normalized_timestamp = normalize_watermark(watermark_at).to_pydatetime()
    connection.execute(
        UPSERT_WATERMARK,
        {
            "pipeline_name": normalized_name,
            "watermark_at": normalized_timestamp,
        },
    )