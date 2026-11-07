"""Run the retail pipeline from local samples, with optional PostgreSQL steps."""

import argparse
import json
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy.engine import Engine

from src.ingestion.api_ingestion import ingest_orders
from src.ingestion.csv_ingestion import ingest_sample_csvs
from src.ingestion.mock_api import serve_mock_api
from src.ingestion.postgres_ingestion import extract_retail_tables
from src.loading.incremental_loader import load_incremental_orders, upsert_products
from src.reporting.pipeline_run import PipelineRun, write_execution_log
from src.reporting.quality_report import build_quality_report, write_quality_report
from src.reporting.reconciliation import (
    build_reconciliation_report,
    write_reconciliation_report,
)
from src.transformation.data_cleaner import CleaningResult, clean_and_deduplicate
from src.transformation.data_layers import write_clean_snapshot, write_curated_snapshot
from src.utils.config import (
    get_project_root,
    load_config,
    load_project_environment,
    resolve_project_path,
)
from src.utils.database import create_database_engine
from src.utils.logger import configure_logging
from src.warehouse.schema_manager import initialize_star_schema, populate_date_dimension
from src.warehouse.scd_type2 import apply_customer_changes

logger = logging.getLogger(__name__)
ENTITIES = ("customers", "products", "orders")


def build_curated_sales(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """Join cleaned facts to customer/product attributes and calculate revenue."""
    curated_columns = [
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "city",
        "state",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "total_amount",
        "order_status",
        "source_system",
        "updated_at",
        "ingestion_timestamp",
    ]
    if orders.empty:
        return pd.DataFrame(columns=curated_columns)

    customer_attributes = customers[
        ["customer_id", "customer_name", "city", "state"]
    ]
    product_attributes = products[
        ["product_id", "product_name", "category", "price"]
    ].rename(columns={"price": "unit_price"})
    curated = orders.merge(
        customer_attributes, on="customer_id", how="left", validate="many_to_one"
    ).merge(product_attributes, on="product_id", how="left", validate="many_to_one")
    curated["total_amount"] = (
        pd.to_numeric(curated["quantity"], errors="raise")
        * pd.to_numeric(curated["unit_price"], errors="raise")
    ).round(2)
    return curated.reindex(columns=curated_columns)


def _combine_source_records(
    csv_records: dict[str, pd.DataFrame],
    api_orders: pd.DataFrame,
    postgres_records: dict[str, pd.DataFrame] | None,
) -> dict[str, pd.DataFrame]:
    """Concatenate same-entity batches while retaining their source metadata."""
    combined = {}
    for entity in ENTITIES:
        batches = [csv_records[entity]]
        if entity == "orders":
            batches.append(api_orders)
        if postgres_records is not None:
            batches.append(postgres_records[entity])
        combined[entity] = pd.concat(batches, ignore_index=True, sort=False)
    return combined


def _load_warehouse(
    engine: Engine,
    cleaning_results: dict[str, CleaningResult],
) -> None:
    """Initialize schema, load dimensions, fill dates, then upsert sales facts."""
    customers = cleaning_results["customers"].clean_records
    products = cleaning_results["products"].clean_records
    orders = cleaning_results["orders"].clean_records

    initialize_star_schema(engine=engine)
    apply_customer_changes(customers, engine=engine)
    upsert_products(products, engine=engine)
    if orders.empty:
        return

    sale_dates = pd.to_datetime(orders["order_date"], errors="raise", utc=True).dt.date
    populate_date_dimension(min(sale_dates), max(sale_dates), engine=engine)
    load_incremental_orders(orders, engine=engine)


def run_pipeline(
    *,
    include_postgres: bool = False,
    load_warehouse: bool = False,
    api_url: str | None = None,
    output_directory: str | Path | None = None,
    log_directory: str | Path | None = None,
) -> PipelineRun:
    """Execute ingestion, quality checks, file layers, reports, and optional DB load.

    The default mode uses bundled CSV files and a local in-process orders API. PostgreSQL
    source extraction and warehouse loading are opt-in so `python main.py` works locally.
    """
    load_project_environment()
    if output_directory is None:
        data_root = get_project_root() / "data"
    else:
        data_root = Path(output_directory)
        if not data_root.is_absolute():
            data_root = resolve_project_path(data_root)
    if log_directory is None:
        log_root = resolve_project_path(
            str(load_config().get("paths", {}).get("logs", "logs"))
        )
    else:
        log_root = Path(log_directory)
        if not log_root.is_absolute():
            log_root = resolve_project_path(log_root)
    configure_logging(log_root)

    run = PipelineRun()
    run_id = run.execution_id
    raw_directory = data_root / "raw"
    quarantine_directory = data_root / "quarantine"
    engine: Engine | None = None

    try:
        if include_postgres or load_warehouse:
            engine = create_database_engine()

        csv_records = ingest_sample_csvs(raw_directory=raw_directory)
        configured_api_url = api_url if api_url is not None else os.getenv("ORDERS_API_URL")
        if configured_api_url:
            api_orders = ingest_orders(
                configured_api_url, raw_directory=raw_directory
            )
        else:
            with serve_mock_api() as local_api_url:
                api_orders = ingest_orders(
                    f"{local_api_url}/api/orders", raw_directory=raw_directory
                )

        postgres_records = None
        if include_postgres:
            if engine is None:
                raise RuntimeError("PostgreSQL extraction requires a database engine.")
            postgres_records = extract_retail_tables(
                engine=engine,
                raw_directory=raw_directory,
            )

        source_records = _combine_source_records(
            csv_records,
            api_orders,
            postgres_records,
        )
        source_counts = {entity: len(source_records[entity]) for entity in ENTITIES}
        run.source_record_count = sum(source_counts.values())
        run.raw_record_count = run.source_record_count

        cleaning_results: dict[str, CleaningResult] = {}
        customer_result = clean_and_deduplicate(
            source_records["customers"],
            "customers",
            quarantine_directory=quarantine_directory,
        )
        product_result = clean_and_deduplicate(
            source_records["products"],
            "products",
            quarantine_directory=quarantine_directory,
        )
        order_result = clean_and_deduplicate(
            source_records["orders"],
            "orders",
            customer_ids=set(customer_result.clean_records["customer_id"]),
            product_ids=set(product_result.clean_records["product_id"]),
            quarantine_directory=quarantine_directory,
        )
        cleaning_results.update(
            customers=customer_result,
            products=product_result,
            orders=order_result,
        )

        quality_reports = {}
        clean_counts = {}
        rejected_counts = {}
        duplicate_counts = {}
        for entity, result in cleaning_results.items():
            clean_records = result.clean_records
            write_clean_snapshot(
                clean_records,
                entity,
                run_id=run_id,
                base_directory=data_root,
            )
            clean_counts[entity] = len(clean_records)
            rejected_counts[entity] = len(result.invalid_records)
            duplicate_counts[entity] = max(
                len(result.validation.valid_records) - len(clean_records), 0
            )
            quality_reports[entity] = build_quality_report(
                entity,
                result.validation,
                source_record_count=source_counts[entity],
                duplicate_count=duplicate_counts[entity],
            )

        curated_sales = build_curated_sales(
            order_result.clean_records,
            customer_result.clean_records,
            product_result.clean_records,
        )
        write_curated_snapshot(
            curated_sales,
            "sales",
            run_id=run_id,
            base_directory=data_root,
        )
        run.clean_record_count = sum(clean_counts.values())
        run.curated_record_count = len(curated_sales)
        run.rejected_record_count = sum(rejected_counts.values())

        write_quality_report(
            quality_reports,
            run_id=run_id,
            output_directory=log_root,
        )
        reconciliation = build_reconciliation_report(
            source_counts,
            clean_counts,
            rejected_counts=rejected_counts,
            duplicate_counts=duplicate_counts,
        )
        write_reconciliation_report(
            reconciliation,
            run_id=run_id,
            output_directory=log_root,
        )

        if load_warehouse:
            if engine is None:
                raise RuntimeError("Warehouse loading requires a database engine.")
            _load_warehouse(engine, cleaning_results)

        run.finish(success=True)
        logger.info(
            "Pipeline %s finished successfully: source=%s clean=%s curated=%s rejected=%s",
            run.execution_id,
            run.source_record_count,
            run.clean_record_count,
            run.curated_record_count,
            run.rejected_record_count,
        )
        return run
    except Exception as error:
        run.finish(success=False, error=error)
        logger.exception("Pipeline %s failed", run.execution_id)
        raise
    finally:
        try:
            write_execution_log(run, log_root)
        finally:
            if engine is not None:
                engine.dispose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the retail omnichannel pipeline.")
    parser.add_argument(
        "--include-postgres",
        action="store_true",
        help="Extract existing customers, products, and orders from PostgreSQL.",
    )
    parser.add_argument(
        "--load-warehouse",
        action="store_true",
        help="Initialize the star schema and load dimensions and sales facts.",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entry point; return a process status code."""
    arguments = _parse_args()
    try:
        run = run_pipeline(
            include_postgres=arguments.include_postgres,
            load_warehouse=arguments.load_warehouse,
        )
    except Exception:
        return 1

    print(json.dumps(run.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())