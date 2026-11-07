"""Validate required retail fields and quarantine invalid source records."""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.utils.config import load_config, resolve_project_path

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "customers": (
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "updated_at",
    ),
    "products": ("product_id", "product_name", "category", "price", "updated_at"),
    "orders": (
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_date",
        "order_status",
        "source_system",
        "updated_at",
    ),
}
DATE_COLUMNS = {
    "customers": ("updated_at",),
    "products": ("updated_at",),
    "orders": ("order_date", "updated_at"),
}
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class SchemaValidationError(ValueError):
    """Raised when an input dataset is missing required columns."""


@dataclass
class ValidationResult:
    """Validated rows, rejected rows, duplicate rows, and null counts."""

    valid_records: pd.DataFrame
    invalid_records: pd.DataFrame
    duplicate_records: pd.DataFrame
    missing_values: dict[str, int]

    @property
    def total_records(self) -> int:
        return len(self.valid_records) + len(self.invalid_records)


def standardize_column_names(records: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names and replace spaces or punctuation with underscores."""
    standardized_names = []
    for column in records.columns:
        name = re.sub(r"[^0-9a-zA-Z_]+", "_", str(column).strip().lower())
        standardized_names.append(re.sub(r"_+", "_", name).strip("_"))

    if len(standardized_names) != len(set(standardized_names)):
        raise ValueError("Column names are not unique after standardization.")

    standardized = records.copy()
    standardized.columns = standardized_names
    return standardized


def _missing_mask(values: pd.Series) -> pd.Series:
    missing = values.isna()
    if pd.api.types.is_object_dtype(values.dtype) or pd.api.types.is_string_dtype(values.dtype):
        blank = values.astype("string").str.strip().eq("").fillna(False)
        missing = missing | blank
    return missing


def _quarantine_path(entity: str, quarantine_directory: str | Path | None) -> Path:
    if quarantine_directory is None:
        configured_path = load_config().get("paths", {}).get(
            "quarantine_data", "data/quarantine"
        )
        output_directory = resolve_project_path(configured_path)
    else:
        output_directory = Path(quarantine_directory)
        if not output_directory.is_absolute():
            output_directory = resolve_project_path(output_directory)

    output_directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return output_directory / f"{entity}_invalid_{timestamp}.csv"


def validate_records(
    records: pd.DataFrame,
    entity: str,
    *,
    customer_ids: Iterable[str] | None = None,
    product_ids: Iterable[str] | None = None,
    quarantine_directory: str | Path | None = None,
) -> ValidationResult:
    """Split records into valid and invalid sets, flag duplicates, and quarantine rejects.

    Order validation requires customer and product IDs so foreign-key checks are explicit.
    Duplicate rows remain valid candidates here; the cleaner selects the latest valid version.
    """
    entity_name = entity.strip().lower()
    if entity_name not in REQUIRED_COLUMNS:
        raise ValueError(f"Unsupported entity '{entity}'.")
    if not isinstance(records, pd.DataFrame):
        raise TypeError("records must be a pandas DataFrame.")
    if entity_name == "orders" and (customer_ids is None or product_ids is None):
        raise ValueError("Order validation requires customer_ids and product_ids.")

    frame = standardize_column_names(records)
    missing_columns = sorted(set(REQUIRED_COLUMNS[entity_name]) - set(frame.columns))
    if missing_columns:
        raise SchemaValidationError(
            f"{entity_name} is missing required columns: {', '.join(missing_columns)}"
        )

    row_errors: list[list[str]] = [[] for _ in range(len(frame))]

    def mark_invalid(mask: pd.Series, reason: str) -> None:
        for position, is_invalid in enumerate(mask.fillna(False).tolist()):
            if is_invalid:
                row_errors[position].append(reason)

    missing_values = {
        column: int(_missing_mask(frame[column]).sum())
        for column in frame.columns
        if _missing_mask(frame[column]).any()
    }
    for column in REQUIRED_COLUMNS[entity_name]:
        mark_invalid(_missing_mask(frame[column]), f"missing:{column}")

    if entity_name == "customers":
        email_values = frame["email"].astype("string").str.strip()
        email_is_valid = email_values.str.fullmatch(EMAIL_PATTERN, na=False)
        mark_invalid(~email_is_valid & ~_missing_mask(frame["email"]), "invalid_email:email")

    positive_numeric_columns = {"products": ("price",), "orders": ("quantity",)}.get(
        entity_name, ()
    )
    for column in positive_numeric_columns:
        numeric_values = pd.to_numeric(frame[column], errors="coerce")
        missing = _missing_mask(frame[column])
        mark_invalid(numeric_values.isna() & ~missing, f"invalid_number:{column}")
        mark_invalid(numeric_values.le(0).fillna(False), f"must_be_positive:{column}")
        if column == "quantity":
            fractional = numeric_values.notna() & numeric_values.mod(1).ne(0)
            mark_invalid(fractional, "must_be_integer:quantity")

    for column in DATE_COLUMNS[entity_name]:
        parsed_dates = pd.to_datetime(frame[column], errors="coerce", utc=True)
        invalid_dates = parsed_dates.isna() & ~_missing_mask(frame[column])
        mark_invalid(invalid_dates, f"invalid_date:{column}")

    if entity_name == "orders":
        known_customers = {str(value).strip() for value in customer_ids if not pd.isna(value)}
        known_products = {str(value).strip() for value in product_ids if not pd.isna(value)}
        order_customer_ids = frame["customer_id"].astype("string").str.strip()
        order_product_ids = frame["product_id"].astype("string").str.strip()
        mark_invalid(
            ~order_customer_ids.isin(known_customers) & ~_missing_mask(frame["customer_id"]),
            "unknown_customer_id:customer_id",
        )
        mark_invalid(
            ~order_product_ids.isin(known_products) & ~_missing_mask(frame["product_id"]),
            "unknown_product_id:product_id",
        )

    error_values = pd.Series(
        ["; ".join(errors) for errors in row_errors], index=frame.index, dtype="string"
    )
    valid_mask = error_values.eq("")
    valid_records = frame.loc[valid_mask].copy()
    invalid_records = frame.loc[~valid_mask].copy()
    if not invalid_records.empty:
        invalid_records["validation_errors"] = error_values.loc[~valid_mask]
        output_path = _quarantine_path(entity_name, quarantine_directory)
        invalid_records.to_csv(output_path, index=False, encoding="utf-8")
        logger.warning(
            "Quarantined %s invalid %s records to %s",
            len(invalid_records),
            entity_name,
            output_path,
        )

    duplicate_records = frame.iloc[0:0].copy()
    if entity_name == "orders":
        duplicate_mask = ~_missing_mask(frame["order_id"]) & frame["order_id"].duplicated(
            keep=False
        )
        duplicate_records = frame.loc[duplicate_mask].copy()

    return ValidationResult(
        valid_records=valid_records,
        invalid_records=invalid_records,
        duplicate_records=duplicate_records,
        missing_values=missing_values,
    )