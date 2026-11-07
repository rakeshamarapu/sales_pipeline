"""Tests for Phase 7 quality, execution, and reconciliation reports."""

import json
from pathlib import Path

import pandas as pd

from src.reporting.pipeline_run import PipelineRun, write_execution_log
from src.reporting.quality_report import build_quality_report, write_quality_report
from src.reporting.reconciliation import (
    build_reconciliation_report,
    write_reconciliation_report,
)
from src.utils.logger import configure_logging
from src.validation.schema_validator import validate_records

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_execution_log_writes_run_counts_and_status(tmp_path: Path) -> None:
    run = PipelineRun(source_record_count=12, raw_record_count=12)
    run.clean_record_count = 10
    run.rejected_record_count = 2
    run.finish(success=False, error="sample failure")

    log_path = write_execution_log(run, tmp_path)
    logged_run = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])

    assert logged_run["execution_id"] == run.execution_id
    assert logged_run["status"] == "FAILED"
    assert logged_run["source_record_count"] == 12
    assert logged_run["clean_record_count"] == 10
    assert logged_run["errors"] == ["sample failure"]
    assert logged_run["end_time"] is not None


def test_quality_report_summarizes_valid_invalid_duplicates_and_missing(
    tmp_path: Path,
) -> None:
    sample_directory = PROJECT_ROOT / "data" / "sample"
    customer_records = pd.read_csv(sample_directory / "customers.csv").head(3)
    order_records = pd.read_csv(sample_directory / "orders.csv").head(1)
    order_records = pd.concat([order_records, order_records], ignore_index=True)
    validation = validate_records(
        order_records,
        "orders",
        customer_ids=set(customer_records["customer_id"]),
        product_ids={"P001"},
        quarantine_directory=tmp_path,
    )

    report = build_quality_report("orders", validation, source_record_count=2)
    output_path = write_quality_report(
        {"orders": report}, run_id="run-001", output_directory=tmp_path
    )

    assert report["total_records_received"] == 2
    assert report["valid_records"] == 2
    assert report["duplicate_records"] == 2
    assert report["rejected_records"] == 0
    assert json.loads(output_path.read_text(encoding="utf-8"))["orders"]["source_record_count"] == 2


def test_reconciliation_accounts_for_rejects_and_deduplicated_rows(
    tmp_path: Path,
) -> None:
    report = build_reconciliation_report(
        {"customers": 10, "orders": 20},
        {"customers": 9, "orders": 15},
        rejected_counts={"customers": 1, "orders": 2},
        duplicate_counts={"orders": 3},
    )
    report_path = write_reconciliation_report(
        report, run_id="run-002", output_directory=tmp_path
    )

    assert set(report["status"]) == {"MATCH"}
    saved_report = pd.read_csv(report_path)
    assert saved_report.loc[saved_report["entity"] == "orders", "unaccounted_count"].item() == 0


def test_reconciliation_marks_unaccounted_rows() -> None:
    report = build_reconciliation_report({"orders": 10}, {"orders": 8})

    assert report.loc[0, "status"] == "MISMATCH"
    assert report.loc[0, "unaccounted_count"] == 2


def test_logging_config_creates_pipeline_log(tmp_path: Path) -> None:
    log_path = configure_logging(tmp_path)

    assert log_path == tmp_path / "pipeline.log"
    assert log_path.exists()


def test_analytics_file_contains_all_ten_queries() -> None:
    analytics_sql = (PROJECT_ROOT / "sql" / "analytics_queries.sql").read_text(
        encoding="utf-8"
    )

    assert all(f"-- Query {query_number}:" in analytics_sql for query_number in range(1, 11))
    assert "WITH monthly_sales AS" in analytics_sql
    assert "RANK() OVER" in analytics_sql
