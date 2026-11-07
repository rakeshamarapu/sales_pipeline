"""Tests for Phase 3 schema validation, quarantine, cleaning, and deduplication."""

from pathlib import Path

import pandas as pd
import pytest

from src.transformation.data_cleaner import clean_and_deduplicate
from src.validation.schema_validator import (
    SchemaValidationError,
    standardize_column_names,
    validate_records,
)


def _customer_records() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "customer_id": "C001",
                "customer_name": "Aarav Sharma",
                "email": "aarav@example.com",
                "city": "Hyderabad",
                "state": "Telangana",
                "updated_at": "2025-01-02T09:15:00Z",
            }
        ]
    )


def _order(order_id: str, updated_at: str, quantity: int = 1) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": quantity,
        "order_date": "2025-01-03T10:15:00Z",
        "order_status": "Delivered",
        "source_system": "web_store",
        "updated_at": updated_at,
        "ingestion_timestamp": "2025-01-04T10:00:00Z",
    }


def test_standardize_column_names() -> None:
    records = pd.DataFrame({" Customer ID ": ["C001"], "Order-Date": ["2025-01-01"]})

    standardized = standardize_column_names(records)

    assert list(standardized.columns) == ["customer_id", "order_date"]


def test_standardize_rejects_colliding_names() -> None:
    records = pd.DataFrame([[1, 2]], columns=["Customer ID", "customer-id"])

    with pytest.raises(ValueError, match="not unique"):
        standardize_column_names(records)


def test_committed_sample_data_passes_validation_and_cleaning(tmp_path: Path) -> None:
    sample_directory = Path(__file__).resolve().parents[1] / "data" / "sample"
    customers = clean_and_deduplicate(
        pd.read_csv(sample_directory / "customers.csv"),
        "customers",
        quarantine_directory=tmp_path,
    )
    products = clean_and_deduplicate(
        pd.read_csv(sample_directory / "products.csv"),
        "products",
        quarantine_directory=tmp_path,
    )
    orders = clean_and_deduplicate(
        pd.read_csv(sample_directory / "orders.csv"),
        "orders",
        customer_ids=set(customers.clean_records["customer_id"]),
        product_ids=set(products.clean_records["product_id"]),
        quarantine_directory=tmp_path,
    )

    assert len(customers.clean_records) == 20
    assert len(products.clean_records) == 15
    assert len(orders.clean_records) == 100
    assert customers.invalid_records.empty
    assert products.invalid_records.empty
    assert orders.invalid_records.empty
    assert orders.duplicate_records.empty


def test_missing_required_columns_raise_clear_error() -> None:
    with pytest.raises(SchemaValidationError, match="missing required columns"):
        validate_records(pd.DataFrame({"customer_id": ["C001"]}), "customers")


def test_invalid_customer_is_quarantined_and_reported(tmp_path: Path) -> None:
    records = pd.concat(
        [
            _customer_records(),
            pd.DataFrame(
                [
                    {
                        "customer_id": "C002",
                        "customer_name": "Bad Email",
                        "email": "not-an-email",
                        "city": "Pune",
                        "state": "Maharashtra",
                        "updated_at": "not-a-date",
                    },
                    {
                        "customer_id": "C003",
                        "customer_name": "Missing City",
                        "email": "valid@example.com",
                        "city": " ",
                        "state": "Karnataka",
                        "updated_at": "2025-01-02T09:15:00Z",
                    },
                ]
            ),
        ],
        ignore_index=True,
    )

    result = validate_records(
        records, "customers", quarantine_directory=tmp_path
    )

    assert len(result.valid_records) == 1
    assert len(result.invalid_records) == 2
    assert result.total_records == 3
    assert result.missing_values["city"] == 1
    assert "invalid_email:email" in result.invalid_records.iloc[0]["validation_errors"]
    assert "invalid_date:updated_at" in result.invalid_records.iloc[0]["validation_errors"]
    quarantine_files = list(tmp_path.glob("customers_invalid_*.csv"))
    assert len(quarantine_files) == 1
    assert len(pd.read_csv(quarantine_files[0])) == 2


def test_order_validation_checks_positive_quantity_and_foreign_keys(
    tmp_path: Path,
) -> None:
    invalid_order = _order("O-2", "2025-01-03T11:00:00Z", quantity=0)
    invalid_order["customer_id"] = "C404"
    invalid_order["product_id"] = "P404"
    invalid_order["order_date"] = "bad-date"

    result = validate_records(
        pd.DataFrame([_order("O-1", "2025-01-03T10:30:00Z"), invalid_order]),
        "orders",
        customer_ids={"C001"},
        product_ids={"P001"},
        quarantine_directory=tmp_path,
    )

    errors = result.invalid_records.iloc[0]["validation_errors"]
    assert "must_be_positive:quantity" in errors
    assert "unknown_customer_id:customer_id" in errors
    assert "unknown_product_id:product_id" in errors
    assert "invalid_date:order_date" in errors


def test_product_validation_rejects_non_positive_price(tmp_path: Path) -> None:
    records = pd.DataFrame(
        [
            {
                "product_id": "P001",
                "product_name": "Test Product",
                "category": "Test",
                "price": -1,
                "updated_at": "2025-01-01T00:00:00Z",
            }
        ]
    )

    result = validate_records(records, "products", quarantine_directory=tmp_path)

    assert result.valid_records.empty
    assert "must_be_positive:price" in result.invalid_records.iloc[0]["validation_errors"]


def test_cleaning_keeps_latest_valid_order_and_removes_invalid_newer_version(
    tmp_path: Path,
) -> None:
    older_valid = _order("O-1", "2025-02-01T10:00:00Z", quantity=1)
    newer_invalid = _order("O-1", "2025-02-03T10:00:00Z", quantity=0)
    older_duplicate = _order("O-2", "2025-02-01T10:00:00Z", quantity=1)
    newer_duplicate = _order("O-2", "2025-02-05T10:00:00Z", quantity=2)
    records = pd.DataFrame(
        [older_valid, newer_invalid, older_duplicate, newer_duplicate]
    ).rename(columns={"order_id": " Order ID "})

    result = clean_and_deduplicate(
        records,
        "orders",
        customer_ids={"C001"},
        product_ids={"P001"},
        quarantine_directory=tmp_path,
    )

    selected = result.clean_records.set_index("order_id")
    assert selected.loc["O-1", "updated_at"] == pd.Timestamp("2025-02-01T10:00:00Z")
    assert selected.loc["O-2", "updated_at"] == pd.Timestamp("2025-02-05T10:00:00Z")
    assert selected.loc["O-2", "quantity"] == 2
    assert result.clean_records["quantity"].dtype == "int64"
    assert len(result.invalid_records) == 1
    assert len(result.duplicate_records) == 2
    assert len(result.validation.duplicate_records) == 4
    assert list(tmp_path.glob("orders_invalid_*.csv"))