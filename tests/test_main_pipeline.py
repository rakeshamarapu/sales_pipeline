"""End-to-end tests for the default file-based pipeline runner."""

import json
from pathlib import Path

import pandas as pd
import pytest

import main as pipeline_main


def test_run_pipeline_builds_file_layers_and_reports(tmp_path: Path) -> None:
    data_directory = tmp_path / "data"
    log_directory = tmp_path / "logs"

    run = pipeline_main.run_pipeline(
        api_url="",
        output_directory=data_directory,
        log_directory=log_directory,
    )

    assert run.status == "SUCCESS"
    assert run.source_record_count == 235
    assert run.raw_record_count == 235
    assert run.clean_record_count == 135
    assert run.curated_record_count == 100
    assert run.rejected_record_count == 0

    clean_orders = pd.read_csv(
        next((data_directory / "clean").glob("orders_*.csv"))
    )
    curated_sales = pd.read_csv(
        next((data_directory / "curated").glob("sales_*.csv"))
    )
    quality = json.loads(
        next(log_directory.glob("quality_report_*.json")).read_text(encoding="utf-8")
    )
    reconciliation = pd.read_csv(next(log_directory.glob("reconciliation_*.csv")))
    execution = json.loads(
        (log_directory / "pipeline_executions.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )

    assert len(clean_orders) == 100
    assert set(curated_sales["customer_name"].dropna())
    assert set(curated_sales["product_name"].dropna())
    assert curated_sales["total_amount"].gt(0).all()
    assert quality["orders"]["total_records_received"] == 200
    assert quality["orders"]["duplicate_records"] == 100
    assert set(reconciliation["status"]) == {"MATCH"}
    assert execution["execution_id"] == run.execution_id
    assert execution["status"] == "SUCCESS"
    assert (data_directory / "raw" / "csv").exists()
    assert (data_directory / "raw" / "api").exists()
    assert (log_directory / "pipeline.log").exists()


def test_failed_pipeline_writes_failed_execution_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_csv_ingestion(**kwargs):
        raise RuntimeError("sample source failure")

    monkeypatch.setattr(pipeline_main, "ingest_sample_csvs", fail_csv_ingestion)
    log_directory = tmp_path / "logs"

    with pytest.raises(RuntimeError, match="sample source failure"):
        pipeline_main.run_pipeline(
            api_url="",
            output_directory=tmp_path / "data",
            log_directory=log_directory,
        )

    failed_run = json.loads(
        (log_directory / "pipeline_executions.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )
    assert failed_run["status"] == "FAILED"
    assert failed_run["errors"] == ["sample source failure"]


def test_warehouse_orchestration_loads_dimensions_dates_then_facts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple] = []

    class _Records:
        def __init__(self, records: pd.DataFrame) -> None:
            self.clean_records = records

    customer_records = pd.DataFrame([{"customer_id": "C001"}])
    product_records = pd.DataFrame([{"product_id": "P001"}])
    order_records = pd.DataFrame(
        [
            {"order_date": "2025-01-03T10:00:00Z"},
            {"order_date": "2025-01-05T10:00:00Z"},
        ]
    )
    cleaning_results = {
        "customers": _Records(customer_records),
        "products": _Records(product_records),
        "orders": _Records(order_records),
    }
    monkeypatch.setattr(
        pipeline_main,
        "initialize_star_schema",
        lambda engine: events.append(("schema", engine)),
    )
    monkeypatch.setattr(
        pipeline_main,
        "apply_customer_changes",
        lambda records, engine: events.append(("customers", records, engine)),
    )
    monkeypatch.setattr(
        pipeline_main,
        "upsert_products",
        lambda records, engine: events.append(("products", records, engine)),
    )
    monkeypatch.setattr(
        pipeline_main,
        "populate_date_dimension",
        lambda start, end, engine: events.append(("dates", start, end, engine)),
    )
    monkeypatch.setattr(
        pipeline_main,
        "load_incremental_orders",
        lambda records, engine: events.append(("facts", records, engine)),
    )
    engine = object()

    pipeline_main._load_warehouse(engine, cleaning_results)

    assert [event[0] for event in events] == [
        "schema",
        "customers",
        "products",
        "dates",
        "facts",
    ]
    assert events[3][1:3] == (pd.Timestamp("2025-01-03").date(), pd.Timestamp("2025-01-05").date())
