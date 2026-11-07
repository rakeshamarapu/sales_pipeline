"""Tests for Phase 4 clean and curated data-layer snapshots."""

from pathlib import Path

import pandas as pd
import pytest

from src.transformation.data_layers import (
    read_latest_snapshot,
    write_clean_snapshot,
    write_curated_snapshot,
    write_layer_snapshot,
)


def test_clean_snapshot_can_be_read_back(tmp_path: Path) -> None:
    records = pd.DataFrame(
        [{"customer_id": "C001", "customer_name": "Aarav Sharma"}]
    )

    snapshot_path = write_clean_snapshot(
        records,
        "customers",
        run_id="run-001",
        base_directory=tmp_path,
    )
    loaded_records = read_latest_snapshot(
        "clean", "customers", base_directory=tmp_path
    )

    assert snapshot_path == tmp_path / "clean" / "customers_run-001.csv"
    pd.testing.assert_frame_equal(loaded_records, records)


def test_same_run_id_replaces_snapshot_instead_of_duplicating(tmp_path: Path) -> None:
    first_records = pd.DataFrame([{"order_id": "O001", "quantity": 1}])
    updated_records = pd.DataFrame([{"order_id": "O001", "quantity": 2}])

    first_path = write_curated_snapshot(
        first_records, "orders", run_id="run-002", base_directory=tmp_path
    )
    second_path = write_curated_snapshot(
        updated_records, "orders", run_id="run-002", base_directory=tmp_path
    )

    assert first_path == second_path
    assert len(list((tmp_path / "curated").glob("orders_*.csv"))) == 1
    assert read_latest_snapshot(
        "curated", "orders", base_directory=tmp_path
    ).loc[0, "quantity"] == 2


def test_curated_snapshots_select_latest_run(tmp_path: Path) -> None:
    write_curated_snapshot(
        pd.DataFrame([{"order_id": "O001", "quantity": 1}]),
        "orders",
        run_id="20250101T000000000000Z",
        base_directory=tmp_path,
    )
    write_curated_snapshot(
        pd.DataFrame([{"order_id": "O002", "quantity": 2}]),
        "orders",
        run_id="20250102T000000000000Z",
        base_directory=tmp_path,
    )

    latest = read_latest_snapshot("curated", "orders", base_directory=tmp_path)

    assert latest.loc[0, "order_id"] == "O002"


def test_layer_writer_rejects_unknown_layers_and_unsafe_names(tmp_path: Path) -> None:
    records = pd.DataFrame([{"order_id": "O001"}])

    with pytest.raises(ValueError, match="only for the clean and curated"):
        write_layer_snapshot(records, "orders", "raw", base_directory=tmp_path)
    with pytest.raises(ValueError, match="entity must start"):
        write_clean_snapshot(records, "../orders", base_directory=tmp_path)
    with pytest.raises(ValueError, match="run_id must start"):
        write_clean_snapshot(records, "orders", run_id="../latest", base_directory=tmp_path)


def test_read_latest_snapshot_reports_empty_layer(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="No clean snapshots found"):
        read_latest_snapshot("clean", "orders", base_directory=tmp_path)