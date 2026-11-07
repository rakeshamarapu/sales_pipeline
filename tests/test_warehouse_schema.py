"""Tests for the PostgreSQL star-schema definition and setup helper."""

from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.warehouse.schema_manager import (
    DEFAULT_SCHEMA_PATH,
    POPULATE_DATES_SQL,
    WarehouseSchemaError,
    initialize_star_schema,
    populate_date_dimension,
)


class _FakeResult:
    rowcount = 3


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.parameters: list[dict[str, date]] = []
        self.fail = False

    def exec_driver_sql(self, statement: str) -> None:
        if self.fail:
            raise SQLAlchemyError("simulated database failure")
        self.statements.append(statement)

    def execute(self, statement, parameters):
        if self.fail:
            raise SQLAlchemyError("simulated database failure")
        self.statements.append(str(statement))
        self.parameters.append(parameters)
        return _FakeResult()


class _FakeEngine:
    def __init__(self, fail: bool = False) -> None:
        self.connection = _FakeConnection()
        self.connection.fail = fail
        self.disposed = False

    @contextmanager
    def begin(self):
        yield self.connection

    def dispose(self) -> None:
        self.disposed = True


def test_schema_contains_dimensions_fact_constraints_and_indexes() -> None:
    schema_sql = DEFAULT_SCHEMA_PATH.read_text(encoding="utf-8").lower()
    standalone_schema = (
        Path(__file__).resolve().parents[1] / "sql" / "create_tables.sql"
    ).read_text(encoding="utf-8")

    assert standalone_schema == DEFAULT_SCHEMA_PATH.read_text(encoding="utf-8")

    for table_name in (
        "dim_customer",
        "dim_product",
        "dim_date",
        "fact_sales",
        "pipeline_watermark",
    ):
        assert f"create table if not exists {table_name}" in schema_sql
    for required_column in (
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "total_amount",
        "source_system",
        "source_updated_at",
        "ingestion_timestamp",
    ):
        assert required_column in schema_sql
    assert "references dim_customer (customer_key)" in schema_sql
    assert "references dim_product (product_key)" in schema_sql
    assert "references dim_date (date_key)" in schema_sql
    assert "create index if not exists ix_fact_sales_date_key" in schema_sql
    assert "effective_end_date >= effective_start_date" in schema_sql
    assert "alter table dim_customer" in schema_sql
    assert "source_updated_at timestamptz not null" in schema_sql
    assert "add column if not exists source_updated_at" in schema_sql


def test_initialize_schema_executes_ddl_in_transaction() -> None:
    engine = _FakeEngine()

    initialize_star_schema(engine=engine)

    assert len(engine.connection.statements) == 1
    assert "CREATE TABLE IF NOT EXISTS fact_sales" in engine.connection.statements[0]
    assert not engine.disposed


def test_initialize_schema_wraps_database_failure() -> None:
    engine = _FakeEngine(fail=True)

    with pytest.raises(WarehouseSchemaError, match="Could not initialize"):
        initialize_star_schema(engine=engine)


def test_populate_date_dimension_uses_inclusive_range_and_ignores_existing_dates() -> None:
    engine = _FakeEngine()

    inserted_count = populate_date_dimension(
        date(2025, 1, 1), date(2025, 1, 3), engine=engine
    )

    assert inserted_count == 3
    assert engine.connection.parameters == [
        {"start_date": date(2025, 1, 1), "end_date": date(2025, 1, 3)}
    ]
    assert "ON CONFLICT (full_date) DO NOTHING" in str(POPULATE_DATES_SQL)


def test_populate_date_dimension_rejects_reversed_range() -> None:
    with pytest.raises(ValueError, match="on or before"):
        populate_date_dimension(date(2025, 1, 2), date(2025, 1, 1), engine=_FakeEngine())


def test_populate_date_dimension_requires_date_not_datetime() -> None:
    with pytest.raises(TypeError, match="datetime.date"):
        populate_date_dimension(
            datetime(2025, 1, 1), date(2025, 1, 2), engine=_FakeEngine()
        )


def test_populate_date_dimension_wraps_database_failure() -> None:
    with pytest.raises(WarehouseSchemaError, match="Could not populate dim_date"):
        populate_date_dimension(
            date(2025, 1, 1), date(2025, 1, 1), engine=_FakeEngine(fail=True)
        )