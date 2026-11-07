"""Incrementally upsert retail dimensions and sales facts into PostgreSQL."""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.loading.watermarks import advance_pipeline_watermark, get_pipeline_watermark
from src.utils.database import create_database_engine

logger = logging.getLogger(__name__)
DEFAULT_ORDER_PIPELINE = "fact_sales_orders"
DEFAULT_PRODUCT_PIPELINE = "dim_product"

UPSERT_PRODUCT = text(
    """
    INSERT INTO dim_product (
        product_id, product_name, category, price, source_updated_at
    )
    VALUES (
        :product_id, :product_name, :category, :price, :source_updated_at
    )
    ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        category = EXCLUDED.category,
        price = EXCLUDED.price,
        source_updated_at = EXCLUDED.source_updated_at
    WHERE dim_product.source_updated_at <= EXCLUDED.source_updated_at
    """
)
LOOKUP_CUSTOMER_KEY = text(
    """
    SELECT customer_key
    FROM dim_customer
    WHERE customer_id = :customer_id
      AND effective_start_date <= :sale_date
      AND (effective_end_date IS NULL OR :sale_date < effective_end_date)
    ORDER BY effective_start_date DESC, customer_key DESC
    LIMIT 1
    """
)
LOOKUP_PRODUCT = text(
    "SELECT product_key, price FROM dim_product WHERE product_id = :product_id"
)
LOOKUP_DATE_KEY = text("SELECT date_key FROM dim_date WHERE full_date = :sale_date")
UPSERT_FACT_SALE = text(
    """
    INSERT INTO fact_sales (
        order_id, customer_key, product_key, date_key, quantity,
        unit_price, total_amount, order_status, source_system, source_updated_at
    )
    VALUES (
        :order_id, :customer_key, :product_key, :date_key, :quantity,
        :unit_price, :total_amount, :order_status, :source_system, :source_updated_at
    )
    ON CONFLICT (order_id) DO UPDATE
    SET customer_key = EXCLUDED.customer_key,
        product_key = EXCLUDED.product_key,
        date_key = EXCLUDED.date_key,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        total_amount = EXCLUDED.total_amount,
        order_status = EXCLUDED.order_status,
        source_system = EXCLUDED.source_system,
        source_updated_at = EXCLUDED.source_updated_at,
        ingestion_timestamp = CURRENT_TIMESTAMP
    WHERE fact_sales.source_updated_at <= EXCLUDED.source_updated_at
    """
)


class IncrementalLoadError(RuntimeError):
    """Raised when an incremental batch cannot be loaded safely."""


class DimensionLookupError(IncrementalLoadError):
    """Raised when a fact row cannot resolve a warehouse dimension key."""


@dataclass
class IncrementalLoadResult:
    """Summary of the rows considered and accepted by a dimension/fact load."""

    source_rows: int = 0
    unique_rows: int = 0
    upserted_rows: int = 0
    ignored_stale_rows: int = 0
    watermark_at: datetime | None = None


def _validated_timestamps(records: pd.DataFrame, column: str) -> pd.Series:
    timestamps = pd.to_datetime(records[column], errors="coerce", utc=True)
    if timestamps.isna().any():
        raise ValueError(f"Incremental rows contain invalid {column} timestamps.")
    return timestamps


def _required_columns(records: pd.DataFrame, required: tuple[str, ...], entity: str) -> None:
    missing_columns = sorted(set(required) - set(records.columns))
    if missing_columns:
        raise ValueError(
            f"{entity} incremental data is missing columns: {', '.join(missing_columns)}"
        )


def upsert_products(
    products: pd.DataFrame,
    *,
    pipeline_name: str = DEFAULT_PRODUCT_PIPELINE,
    engine: Engine | None = None,
) -> IncrementalLoadResult:
    """Type-1 upsert product rows and advance their watermark in one transaction."""
    if not isinstance(products, pd.DataFrame):
        raise TypeError("products must be a pandas DataFrame.")
    if products.empty:
        return IncrementalLoadResult()

    required = ("product_id", "product_name", "category", "price", "updated_at")
    _required_columns(products, required, "product")
    rows = products.loc[:, list(required)].copy()
    rows["product_id"] = rows["product_id"].astype("string").str.strip()
    if rows["product_id"].isna().any() or rows["product_id"].eq("").any():
        raise ValueError("product_id cannot be null or blank.")

    rows["_source_updated_at"] = _validated_timestamps(rows, "updated_at")
    rows["price"] = pd.to_numeric(rows["price"], errors="coerce")
    if rows["price"].isna().any() or rows["price"].le(0).any():
        raise ValueError("Product prices must be valid positive numbers.")
    for column in ("product_name", "category"):
        rows[column] = rows[column].astype("string").str.strip()
        if rows[column].isna().any() or rows[column].eq("").any():
            raise ValueError(f"{column} cannot be null or blank.")

    rows = rows.sort_values(
        ["product_id", "_source_updated_at"], kind="mergesort"
    ).drop_duplicates("product_id", keep="last")
    latest_update = rows["_source_updated_at"].max().to_pydatetime()
    result = IncrementalLoadResult(
        source_rows=len(products), unique_rows=len(rows), watermark_at=latest_update
    )

    owns_engine = engine is None
    active_engine = engine if engine is not None else create_database_engine()
    try:
        with active_engine.begin() as connection:
            for record in rows.to_dict(orient="records"):
                price = Decimal(str(record["price"])).quantize(Decimal("0.01"))
                upsert_result = connection.execute(
                    UPSERT_PRODUCT,
                    {
                        "product_id": record["product_id"],
                        "product_name": record["product_name"],
                        "category": record["category"],
                        "price": price,
                        "source_updated_at": record["_source_updated_at"].to_pydatetime(),
                    },
                )
                result.upserted_rows += max(upsert_result.rowcount, 0)
            advance_pipeline_watermark(connection, pipeline_name, latest_update)
    except (SQLAlchemyError, InvalidOperation) as error:
        logger.exception("Product incremental load failed")
        raise IncrementalLoadError(
            "Product rows and watermark were not committed; check the database and product data."
        ) from error
    finally:
        if owns_engine:
            active_engine.dispose()

    return result


def load_incremental_orders(
    orders: pd.DataFrame,
    *,
    pipeline_name: str = DEFAULT_ORDER_PIPELINE,
    engine: Engine | None = None,
) -> IncrementalLoadResult:
    """Resolve dimensions, upsert facts, and advance the watermark atomically."""
    if not isinstance(orders, pd.DataFrame):
        raise TypeError("orders must be a pandas DataFrame.")
    if orders.empty:
        return IncrementalLoadResult()

    required = (
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "order_date",
        "order_status",
        "source_system",
        "updated_at",
    )
    _required_columns(orders, required, "order")
    rows = orders.loc[:, list(required)].copy()
    for column in ("order_id", "customer_id", "product_id", "order_status", "source_system"):
        rows[column] = rows[column].astype("string").str.strip()
        if rows[column].isna().any() or rows[column].eq("").any():
            raise ValueError(f"{column} cannot be null or blank.")

    rows["_source_updated_at"] = _validated_timestamps(rows, "updated_at")
    rows["_sale_timestamp"] = pd.to_datetime(rows["order_date"], errors="coerce", utc=True)
    if rows["_sale_timestamp"].isna().any():
        raise ValueError("Incremental orders contain invalid order_date values.")
    rows["quantity"] = pd.to_numeric(rows["quantity"], errors="coerce")
    if (
        rows["quantity"].isna().any()
        or rows["quantity"].le(0).any()
        or rows["quantity"].mod(1).ne(0).any()
    ):
        raise ValueError("Order quantities must be positive whole numbers.")
    rows["quantity"] = rows["quantity"].astype("int64")

    rows = rows.sort_values(
        ["order_id", "_source_updated_at"], kind="mergesort"
    ).drop_duplicates("order_id", keep="last")
    latest_update = rows["_source_updated_at"].max().to_pydatetime()
    result = IncrementalLoadResult(
        source_rows=len(orders), unique_rows=len(rows), watermark_at=latest_update
    )

    owns_engine = engine is None
    active_engine = engine if engine is not None else create_database_engine()
    try:
        with active_engine.begin() as connection:
            for record in rows.to_dict(orient="records"):
                sale_date = record["_sale_timestamp"].date()
                customer_result = connection.execute(
                    LOOKUP_CUSTOMER_KEY,
                    {"customer_id": record["customer_id"], "sale_date": sale_date},
                )
                customer = customer_result.mappings().one_or_none()
                if customer is None:
                    raise DimensionLookupError(
                        f"No effective customer dimension row for "
                        f"{record['customer_id']} on {sale_date}."
                    )

                product_result = connection.execute(
                    LOOKUP_PRODUCT, {"product_id": record["product_id"]}
                )
                product = product_result.mappings().one_or_none()
                if product is None:
                    raise DimensionLookupError(
                        f"No product dimension row for {record['product_id']}."
                    )

                date_result = connection.execute(
                    LOOKUP_DATE_KEY, {"sale_date": sale_date}
                )
                date_row = date_result.mappings().one_or_none()
                if date_row is None:
                    raise DimensionLookupError(
                        f"No date dimension row for {sale_date}; populate dim_date first."
                    )

                unit_price = Decimal(str(product["price"])).quantize(Decimal("0.01"))
                quantity = int(record["quantity"])
                total_amount = (unit_price * quantity).quantize(Decimal("0.01"))
                upsert_result = connection.execute(
                    UPSERT_FACT_SALE,
                    {
                        "order_id": record["order_id"],
                        "customer_key": customer["customer_key"],
                        "product_key": product["product_key"],
                        "date_key": date_row["date_key"],
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "total_amount": total_amount,
                        "order_status": record["order_status"],
                        "source_system": record["source_system"],
                        "source_updated_at": record[
                            "_source_updated_at"
                        ].to_pydatetime(),
                    },
                )
                if upsert_result.rowcount > 0:
                    result.upserted_rows += 1
                else:
                    result.ignored_stale_rows += 1
            advance_pipeline_watermark(connection, pipeline_name, latest_update)
    except (SQLAlchemyError, InvalidOperation) as error:
        logger.exception("Incremental fact load failed")
        raise IncrementalLoadError(
            "Fact rows and watermark were not committed; check the database and dimension rows."
        ) from error
    finally:
        if owns_engine:
            active_engine.dispose()

    logger.info(
        "Incremental sales load: %s source rows, %s unique orders, %s upserted, %s stale",
        result.source_rows,
        result.unique_rows,
        result.upserted_rows,
        result.ignored_stale_rows,
    )
    return result


__all__ = [
    "DEFAULT_ORDER_PIPELINE",
    "DEFAULT_PRODUCT_PIPELINE",
    "DimensionLookupError",
    "IncrementalLoadError",
    "IncrementalLoadResult",
    "get_pipeline_watermark",
    "load_incremental_orders",
    "upsert_products",
]