"""Focused tests for the Phase 2 ingestion modules."""

from http.server import ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import Mock

import pandas as pd
import pytest
import requests
from sqlalchemy.exc import SQLAlchemyError

from src.ingestion.api_ingestion import ApiIngestionError, ingest_orders
from src.ingestion.csv_ingestion import ingest_csv_file
from src.ingestion.mock_api import create_handler
from src.ingestion.postgres_ingestion import (
    PostgresExtractionError,
    extract_table,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_csv_ingestion_adds_metadata_and_writes_raw_snapshot(tmp_path: Path) -> None:
    source_file = PROJECT_ROOT / "data" / "sample" / "customers.csv"

    records = ingest_csv_file(source_file, raw_directory=tmp_path)

    assert len(records) == 20
    assert set(records["source_system"]) == {"csv"}
    assert set(records["ingestion_source"]) == {"csv"}
    assert records["ingestion_timestamp"].notna().all()
    assert len(list((tmp_path / "csv").glob("customers_*.csv"))) == 1


def test_csv_ingestion_preserves_existing_order_source_system(tmp_path: Path) -> None:
    source_file = PROJECT_ROOT / "data" / "sample" / "orders.csv"

    records = ingest_csv_file(source_file, raw_directory=tmp_path)

    assert records.loc[0, "source_system"] == "web_store"
    assert set(records["ingestion_source"]) == {"csv"}


def test_csv_ingestion_reports_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does not exist"):
        ingest_csv_file(tmp_path / "missing.csv", raw_directory=tmp_path / "raw")


def test_api_ingestion_adds_metadata_and_saves_raw_snapshot(tmp_path: Path) -> None:
    response = Mock()
    response.json.return_value = {
        "orders": [
            {
                "order_id": "API-1",
                "source_system": "web_store",
                "quantity": 2,
            }
        ]
    }
    session = Mock()
    session.get.return_value = response

    records = ingest_orders(
        "http://mock.test/api/orders",
        raw_directory=tmp_path,
        session=session,
    )

    session.get.assert_called_once_with("http://mock.test/api/orders", timeout=30)
    assert records.loc[0, "source_system"] == "web_store"
    assert records.loc[0, "ingestion_source"] == "api"
    assert list((tmp_path / "api").glob("orders_*.csv"))


def test_api_ingestion_reports_http_failure(tmp_path: Path) -> None:
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("service unavailable")
    session = Mock()
    session.get.return_value = response

    with pytest.raises(ApiIngestionError, match="request failed"):
        ingest_orders(
            "http://mock.test/api/orders",
            raw_directory=tmp_path,
            session=session,
        )


def test_mock_api_serves_sample_orders(tmp_path: Path) -> None:
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        create_handler(PROJECT_ROOT / "data" / "sample" / "orders.csv"),
    )
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        response = requests.get(
            f"http://127.0.0.1:{server.server_port}/api/orders", timeout=5
        )
        response.raise_for_status()
        payload = response.json()
        ingested_orders = ingest_orders(
            f"http://127.0.0.1:{server.server_port}/api/orders",
            raw_directory=tmp_path,
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    assert len(payload["orders"]) == 100
    assert isinstance(payload["orders"][0]["quantity"], int)
    assert len(ingested_orders) == 100
    assert set(ingested_orders["ingestion_source"]) == {"api"}


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False


class _FakeEngine:
    def connect(self):
        return _FakeConnection()

    def dispose(self) -> None:
        pass


def test_postgres_extraction_adds_metadata_and_saves_raw_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        "src.ingestion.postgres_ingestion.pd.read_sql_query",
        lambda query, connection: pd.DataFrame(
            [{"customer_id": "C001", "customer_name": "Aarav Sharma"}]
        ),
    )

    records = extract_table("customers", engine=_FakeEngine(), raw_directory=tmp_path)

    assert records.loc[0, "source_system"] == "postgres"
    assert records.loc[0, "ingestion_source"] == "postgres"
    assert list((tmp_path / "postgres").glob("customers_*.csv"))


def test_postgres_extraction_rejects_unlisted_table() -> None:
    with pytest.raises(ValueError, match="Unsupported table"):
        extract_table("orders; DROP TABLE customers")


def test_postgres_extraction_reports_connection_failure(
    tmp_path: Path,
) -> None:
    class FailedEngine:
        def connect(self):
            raise SQLAlchemyError("database is unavailable")

        def dispose(self) -> None:
            pass

    with pytest.raises(PostgresExtractionError, match="Check the database settings"):
        extract_table("orders", engine=FailedEngine(), raw_directory=tmp_path)