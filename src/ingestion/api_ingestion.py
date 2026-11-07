"""Fetch JSON orders from the REST API and persist them in the raw layer."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.ingestion.common import add_ingestion_metadata, default_raw_directory, write_raw_snapshot
from src.utils.config import load_config, load_project_environment

logger = logging.getLogger(__name__)


class ApiIngestionError(RuntimeError):
    """Raised when the orders API request or response is invalid."""


def ingest_orders(
    api_url: str | None = None,
    *,
    raw_directory: str | Path | None = None,
    timeout_seconds: int | None = None,
    session: Any = requests,
) -> pd.DataFrame:
    """Fetch orders from the configured API, annotate them, and save a raw snapshot."""
    config = load_config()
    ingestion_config = config.get("ingestion", {})
    load_project_environment()
    url_variable = ingestion_config.get("orders_api_url_env", "ORDERS_API_URL")
    url = api_url or os.getenv(
        url_variable,
        ingestion_config.get(
            "default_orders_api_url", "http://127.0.0.1:5000/api/orders"
        ),
    )
    timeout = timeout_seconds or ingestion_config.get("api_timeout_seconds", 30)

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as error:
        logger.exception("Orders API request failed: %s", url)
        raise ApiIngestionError(f"Orders API request failed: {url}") from error

    if isinstance(payload, dict):
        records = payload.get("orders")
    else:
        records = payload
    if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
        raise ApiIngestionError(
            "Orders API response must be a list of order objects or an object with an 'orders' list."
        )

    timestamp = datetime.now(timezone.utc)
    enriched = add_ingestion_metadata(pd.DataFrame(records), "api", timestamp)
    write_raw_snapshot(
        enriched,
        "orders",
        "api",
        timestamp,
        raw_directory or default_raw_directory(),
    )
    logger.info("Ingested %s orders from API %s", len(enriched), url)
    return enriched