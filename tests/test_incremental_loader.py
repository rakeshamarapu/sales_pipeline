"""Tests for incremental dimension/fact loading and watermark behavior."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.loading.incremental_loader import (
    DimensionLookupError,
    load_incremental_orders,
    upsert_products,
)
from src.loading.watermarks import get_pipeline_watermark


class _Result:
    def __init__(self, value=None, rowcount: int = 1) -> None:
        self.value = value
        self.rowcount = rowcount

    def mappings(self):
        return self

    def one_or_none(self):
        return self.value

    def scalar_one_or_none(self):
        return self.value


class _Connection:
    def __init__(self) -> None:
        self.products = {"P001": {"product_key": 20, "price": Decimal("12.50")}}
        self.product_updates = {"P001": datetime(2025, 1, 1, tzinfo=timezone.utc)}
        self.customers = {"C001": 10}
        self.dates = {date(2025, 1, 3): 20250103}
        self.facts: dict[str, dict[str, object]] = {}
        self.watermarks: dict[str, datetime] = {}
        self.missing_product = False
        self.missing_customer = False
        self.missing_date = False
        self.fail_on_fact = False

    def execute(self, statement, parameters):
        sql = str(statement).lower()
        if "select watermark_at" in sql:
            return _Result(self.watermarks.get(parameters["pipeline_name"]))
        if "select customer_key" in sql:
            if self.missing_customer:
                return _Result(None)
            return _Result({"customer_key": self.customers[parameters["customer_id"]]})
        if "select product_key" in sql:
            if self.missing_product or parameters["product_id"] not in self.products:
                return _Result(None)
            return _Result(self.products[parameters["product_id"]])
        if "select date_key" in sql:
            if self.missing_date or parameters["sale_date"] not in self.dates:
                return _Result(None)
            return _Result({"date_key": self.dates[parameters["sale_date"]]})
        if "insert into dim_product" in sql:
            product_id = parameters["product_id"]
            current_update = self.product_updates.get(product_id)
            source_update = parameters["source_updated_at"]
            if current_update is not None and current_update > source_update:
                return _Result(rowcount=0)
            self.products.setdefault(product_id, {"product_key": 21})
            self.products[product_id].update(
                {
                    "product_name": parameters["product_name"],
                    "category": parameters["category"],
                    "price": parameters["price"],
                }
            )
            self.product_updates[product_id] = source_update
            return _Result(rowcount=1)
        if "insert into fact_sales" in sql:
            if self.fail_on_fact:
                raise SQLAlchemyError("simulated fact insert failure")
            order_id = parameters["order_id"]
            current = self.facts.get(order_id)
            if current and current["source_updated_at"] > parameters["source_updated_at"]:
                return _Result(rowcount=0)
            self.facts[order_id] = dict(parameters)
            return _Result(rowcount=1)
        if "insert into pipeline_watermark" in sql:
            name = parameters["pipeline_name"]
            watermark = parameters["watermark_at"]
            self.watermarks[name] = max(
                self.watermarks.get(name, watermark), watermark
            )
            return _Result()
        raise AssertionError(f"Unexpected SQL: {statement}")


class _Engine:
    def __init__(self) -> None:
        self.connection = _Connection()

    @contextmanager
    def begin(self):
        original_state = deepcopy(self.connection.__dict__)
        try:
            yield self.connection
        except Exception:
            self.connection.__dict__.clear()
            self.connection.__dict__.update(original_state)
            raise

    @contextmanager
    def connect(self):
        yield self.connection

    def dispose(self) -> None:
        pass


def _order(
    order_id: str = "O001",
    updated_at: str = "2025-01-03T11:00:00Z",
    quantity: int = 2,
) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": quantity,
        "order_date": "2025-01-03T10:00:00Z",
        "order_status": "Delivered",
        "source_system": "web_store",
        "updated_at": updated_at,
    }


def test_incremental_fact_upsert_calculates_revenue_and_advances_watermark() -> None:
    engine = _Engine()

    result = load_incremental_orders(pd.DataFrame([_order()]), engine=engine)

    fact = engine.connection.facts["O001"]
    assert result.source_rows == result.unique_rows == result.upserted_rows == 1
    assert result.ignored_stale_rows == 0
    assert fact["customer_key"] == 10
    assert fact["product_key"] == 20
    assert fact["date_key"] == 20250103
    assert fact["unit_price"] == Decimal("12.50")
    assert fact["total_amount"] == Decimal("25.00")
    assert engine.connection.watermarks["fact_sales_orders"] == result.watermark_at


def test_incremental_fact_load_is_idempotent_and_keeps_latest_batch_version() -> None:
    engine = _Engine()
    orders = pd.DataFrame(
        [
            _order(quantity=1, updated_at="2025-01-03T10:00:00Z"),
            _order(quantity=3, updated_at="2025-01-03T12:00:00Z"),
        ]
    )

    result = load_incremental_orders(orders, engine=engine)
    load_incremental_orders(orders, engine=engine)

    assert result.source_rows == 2
    assert result.unique_rows == 1
    assert len(engine.connection.facts) == 1
    assert engine.connection.facts["O001"]["quantity"] == 3
    assert engine.connection.facts["O001"]["total_amount"] == Decimal("37.50")


def test_older_replayed_order_does_not_replace_newer_fact() -> None:
    engine = _Engine()
    load_incremental_orders(
        pd.DataFrame([_order(quantity=3, updated_at="2025-01-03T12:00:00Z")]),
        engine=engine,
    )

    result = load_incremental_orders(
        pd.DataFrame([_order(quantity=1, updated_at="2025-01-03T10:00:00Z")]),
        engine=engine,
    )

    assert result.ignored_stale_rows == 1
    assert engine.connection.facts["O001"]["quantity"] == 3
    assert engine.connection.watermarks["fact_sales_orders"] == datetime(
        2025, 1, 3, 12, 0, tzinfo=timezone.utc
    )


def test_missing_dimension_aborts_batch_without_advancing_watermark() -> None:
    engine = _Engine()
    engine.connection.missing_date = True

    with pytest.raises(DimensionLookupError, match="populate dim_date"):
        load_incremental_orders(pd.DataFrame([_order()]), engine=engine)

    assert not engine.connection.facts
    assert not engine.connection.watermarks


def test_product_upsert_keeps_latest_source_version_and_watermark() -> None:
    engine = _Engine()
    products = pd.DataFrame(
        [
            {
                "product_id": "P001",
                "product_name": "Old Name",
                "category": "Old Category",
                "price": 10,
                "updated_at": "2025-01-02T00:00:00Z",
            },
            {
                "product_id": "P001",
                "product_name": "New Name",
                "category": "Electronics",
                "price": 15,
                "updated_at": "2025-01-04T00:00:00Z",
            },
        ]
    )

    result = upsert_products(products, engine=engine)

    assert result.source_rows == 2
    assert result.unique_rows == result.upserted_rows == 1
    assert engine.connection.products["P001"]["product_name"] == "New Name"
    assert engine.connection.products["P001"]["price"] == Decimal("15.00")
    assert engine.connection.watermarks["dim_product"] == result.watermark_at


def test_watermark_can_be_read_before_and_after_a_load() -> None:
    engine = _Engine()

    assert get_pipeline_watermark("fact_sales_orders", engine=engine) is None
    load_incremental_orders(pd.DataFrame([_order()]), engine=engine)
    watermark = get_pipeline_watermark("fact_sales_orders", engine=engine)

    assert watermark == datetime(2025, 1, 3, 11, 0, tzinfo=timezone.utc)


def test_empty_incremental_batch_does_not_open_database() -> None:
    result = load_incremental_orders(pd.DataFrame(), engine=_Engine())

    assert result.source_rows == result.upserted_rows == 0
    assert result.watermark_at is None