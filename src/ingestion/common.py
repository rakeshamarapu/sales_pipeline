"""Shared metadata and raw-snapshot helpers for all ingestion sources."""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.config import load_config, resolve_project_path


def default_raw_directory() -> Path:
    """Return the configured raw-data directory."""
    raw_path = load_config().get("paths", {}).get("raw_data", "data/raw")
    return resolve_project_path(raw_path)


def add_ingestion_metadata(
    records: pd.DataFrame,
    ingestion_source: str,
    ingestion_timestamp: datetime | None = None,
) -> pd.DataFrame:
    """Add ingestion metadata while retaining any source_system already present."""
    enriched = records.copy()
    timestamp = ingestion_timestamp or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    if "source_system" not in enriched.columns:
        enriched["source_system"] = ingestion_source
    enriched["ingestion_source"] = ingestion_source
    enriched["ingestion_timestamp"] = timestamp.isoformat().replace("+00:00", "Z")
    return enriched


def normalize_watermark(value: datetime | str) -> pd.Timestamp:
    """Convert a watermark to a timezone-aware UTC pandas timestamp."""
    watermark = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(watermark):
        raise ValueError(f"Invalid incremental watermark: {value!r}")
    return watermark


def filter_records_since(
    records: pd.DataFrame,
    watermark: datetime | str,
    timestamp_column: str = "updated_at",
) -> pd.DataFrame:
    """Keep rows at or after a watermark, including rows with invalid timestamps.

    Invalid timestamp rows are retained so schema validation can quarantine them.
    The inclusive boundary makes equal-timestamp replays safe when loads are upserts.
    """
    if timestamp_column not in records.columns:
        raise ValueError(
            f"Incremental extraction requires a '{timestamp_column}' column."
        )

    normalized_watermark = normalize_watermark(watermark)
    parsed_timestamps = pd.to_datetime(
        records[timestamp_column], errors="coerce", utc=True
    )
    keep_mask = parsed_timestamps.isna() | parsed_timestamps.ge(normalized_watermark)
    return records.loc[keep_mask].copy()


def write_raw_snapshot(
    records: pd.DataFrame,
    entity: str,
    ingestion_source: str,
    ingestion_timestamp: datetime,
    raw_directory: str | Path | None = None,
) -> Path:
    """Save an immutable CSV snapshot and return the file path."""
    output_directory = Path(raw_directory) if raw_directory is not None else default_raw_directory()
    if not output_directory.is_absolute():
        output_directory = resolve_project_path(output_directory)

    source_directory = output_directory / ingestion_source
    source_directory.mkdir(parents=True, exist_ok=True)
    file_timestamp = ingestion_timestamp.astimezone(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )
    output_path = source_directory / f"{entity}_{file_timestamp}.csv"
    records.to_csv(output_path, index=False, encoding="utf-8")
    return output_path