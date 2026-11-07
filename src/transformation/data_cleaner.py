"""Normalize validated retail records and deduplicate by business key."""

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.validation.schema_validator import (
    ValidationResult,
    standardize_column_names,
    validate_records,
)

logger = logging.getLogger(__name__)
BUSINESS_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
}
DATETIME_COLUMNS = ("order_date", "updated_at", "ingestion_timestamp")
NUMERIC_COLUMNS = ("price", "quantity")


@dataclass
class CleaningResult:
    """Clean records and the validation details used to produce them."""

    clean_records: pd.DataFrame
    invalid_records: pd.DataFrame
    duplicate_records: pd.DataFrame
    validation: ValidationResult


def _normalize_values(records: pd.DataFrame) -> pd.DataFrame:
    cleaned = records.copy()
    for column in cleaned.columns:
        values = cleaned[column]
        if pd.api.types.is_object_dtype(values.dtype) or pd.api.types.is_string_dtype(values.dtype):
            normalized_values = values.astype("string").str.strip()
            cleaned[column] = normalized_values.mask(normalized_values.eq(""), pd.NA)

    for column in NUMERIC_COLUMNS:
        if column in cleaned.columns:
            cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    for column in DATETIME_COLUMNS:
        if column in cleaned.columns:
            cleaned[column] = pd.to_datetime(cleaned[column], errors="coerce", utc=True)
    return cleaned


def clean_and_deduplicate(
    records: pd.DataFrame,
    entity: str,
    *,
    customer_ids: set[str] | None = None,
    product_ids: set[str] | None = None,
    quarantine_directory: str | Path | None = None,
) -> CleaningResult:
    """Clean records, quarantine invalid rows, and keep the latest valid key version."""
    entity_name = entity.strip().lower()
    if entity_name not in BUSINESS_KEYS:
        raise ValueError(f"Unsupported entity '{entity}'.")

    normalized_records = _normalize_values(standardize_column_names(records))
    validation = validate_records(
        normalized_records,
        entity_name,
        customer_ids=customer_ids,
        product_ids=product_ids,
        quarantine_directory=quarantine_directory,
    )
    valid_records = validation.valid_records.copy()
    business_key = BUSINESS_KEYS[entity_name]
    duplicate_mask = valid_records[business_key].duplicated(keep=False)
    duplicate_records = valid_records.loc[duplicate_mask].copy()

    sort_columns = [business_key, "updated_at"]
    ascending = [True, False]
    if "ingestion_timestamp" in valid_records.columns:
        sort_columns.append("ingestion_timestamp")
        ascending.append(False)

    clean_records = (
        valid_records.sort_values(
            sort_columns,
            ascending=ascending,
            na_position="last",
            kind="mergesort",
        )
        .drop_duplicates(subset=[business_key], keep="first")
        .sort_values(business_key, kind="mergesort")
        .reset_index(drop=True)
    )
    if entity_name == "orders":
        clean_records["quantity"] = clean_records["quantity"].astype("int64")

    logger.info(
        "Cleaned %s records: %s valid, %s invalid, %s duplicate candidates",
        entity_name,
        len(clean_records),
        len(validation.invalid_records),
        len(duplicate_records),
    )
    return CleaningResult(
        clean_records=clean_records,
        invalid_records=validation.invalid_records,
        duplicate_records=duplicate_records,
        validation=validation,
    )