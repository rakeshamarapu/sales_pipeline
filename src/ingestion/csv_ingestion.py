"""Read source CSV files, attach ingestion metadata, and save raw snapshots."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.ingestion.common import (
    add_ingestion_metadata,
    default_raw_directory,
    filter_records_since,
    write_raw_snapshot,
)
from src.utils.config import load_config, resolve_project_path

logger = logging.getLogger(__name__)


class CsvIngestionError(RuntimeError):
    """Raised when a source CSV cannot be read."""


def ingest_csv_file(
    file_path: str | Path,
    *,
    entity: str | None = None,
    raw_directory: str | Path | None = None,
    updated_since: datetime | str | None = None,
) -> pd.DataFrame:
    """Read one CSV, add metadata, and persist the original rows in the raw layer."""
    source_path = resolve_project_path(file_path)
    if not source_path.is_file():
        logger.error("CSV source file does not exist: %s", source_path)
        raise FileNotFoundError(f"CSV source file does not exist: {source_path}")

    try:
        records = pd.read_csv(source_path)
    except (OSError, UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        logger.exception("Could not read CSV source file: %s", source_path)
        raise CsvIngestionError(f"Could not read CSV source file: {source_path}") from error

    if updated_since is not None:
        try:
            records = filter_records_since(records, updated_since)
        except ValueError as error:
            raise CsvIngestionError(
                f"Could not incrementally filter CSV source file: {source_path}"
            ) from error

    entity_name = entity or source_path.stem
    timestamp = datetime.now(timezone.utc)
    enriched = add_ingestion_metadata(records, "csv", timestamp)
    write_raw_snapshot(
        enriched,
        entity_name,
        "csv",
        timestamp,
        raw_directory or default_raw_directory(),
    )
    logger.info("Ingested %s CSV records from %s", len(enriched), source_path)
    return enriched


def ingest_sample_csvs(
    sample_directory: str | Path | None = None,
    raw_directory: str | Path | None = None,
    updated_since: datetime | str | None = None,
) -> dict[str, pd.DataFrame]:
    """Ingest the sample customer, product, and order CSV files."""
    if sample_directory is None:
        configured_path = load_config().get("paths", {}).get("sample_data", "data/sample")
        sample_path = resolve_project_path(configured_path)
    else:
        sample_path = resolve_project_path(sample_directory)

    return {
        entity: ingest_csv_file(
            sample_path / f"{entity}.csv",
            entity=entity,
            raw_directory=raw_directory,
            updated_since=updated_since,
        )
        for entity in ("customers", "products", "orders")
    }