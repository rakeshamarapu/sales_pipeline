"""Persist cleaned and prepared records in the project's file-based data layers."""

import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import pandas as pd

from src.utils.config import load_config, resolve_project_path

logger = logging.getLogger(__name__)
LayerName = Literal["clean", "curated"]
LAYER_CONFIG_KEYS = {"clean": "clean_data", "curated": "curated_data"}
SAFE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def _validate_name(value: str, label: str) -> str:
    normalized = value.strip()
    if not SAFE_NAME_PATTERN.fullmatch(normalized):
        raise ValueError(
            f"{label} must start with a letter or digit and contain only letters, "
            "digits, underscores, and hyphens."
        )
    return normalized


def _layer_directory(layer: LayerName, base_directory: str | Path | None) -> Path:
    if layer not in LAYER_CONFIG_KEYS:
        raise ValueError("File snapshots are supported only for the clean and curated layers.")

    if base_directory is None:
        configured_path = load_config().get("paths", {}).get(
            LAYER_CONFIG_KEYS[layer], f"data/{layer}"
        )
        return resolve_project_path(configured_path)

    root = Path(base_directory)
    if not root.is_absolute():
        root = resolve_project_path(root)
    return root / layer


def write_layer_snapshot(
    records: pd.DataFrame,
    entity: str,
    layer: LayerName,
    *,
    run_id: str | None = None,
    base_directory: str | Path | None = None,
) -> Path:
    """Atomically write one entity snapshot and return its path.

    A caller-provided run ID makes writes repeatable: writing the same entity, layer,
    and run ID replaces that snapshot instead of creating another file.
    """
    if not isinstance(records, pd.DataFrame):
        raise TypeError("records must be a pandas DataFrame.")

    entity_name = _validate_name(entity, "entity")
    snapshot_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    snapshot_id = _validate_name(snapshot_id, "run_id")
    output_directory = _layer_directory(layer, base_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    output_path = output_directory / f"{entity_name}_{snapshot_id}.csv"
    temporary_path = output_directory / f".{uuid.uuid4().hex}.tmp"
    try:
        records.to_csv(temporary_path, index=False, encoding="utf-8")
        os.replace(temporary_path, output_path)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        logger.exception("Could not write %s layer snapshot for %s", layer, entity_name)
        raise

    logger.info(
        "Wrote %s records to %s layer snapshot %s",
        len(records),
        layer,
        output_path,
    )
    return output_path


def write_clean_snapshot(
    records: pd.DataFrame,
    entity: str,
    *,
    run_id: str | None = None,
    base_directory: str | Path | None = None,
) -> Path:
    """Persist records returned by data cleaning to the clean layer."""
    return write_layer_snapshot(
        records,
        entity,
        "clean",
        run_id=run_id,
        base_directory=base_directory,
    )


def write_curated_snapshot(
    records: pd.DataFrame,
    entity: str,
    *,
    run_id: str | None = None,
    base_directory: str | Path | None = None,
) -> Path:
    """Persist caller-prepared records to the curated layer."""
    return write_layer_snapshot(
        records,
        entity,
        "curated",
        run_id=run_id,
        base_directory=base_directory,
    )


def read_latest_snapshot(
    layer: LayerName,
    entity: str,
    *,
    base_directory: str | Path | None = None,
) -> pd.DataFrame:
    """Read the most recently named snapshot for an entity and layer."""
    entity_name = _validate_name(entity, "entity")
    output_directory = _layer_directory(layer, base_directory)
    snapshots = sorted(output_directory.glob(f"{entity_name}_*.csv"), reverse=True)
    if not snapshots:
        raise FileNotFoundError(
            f"No {layer} snapshots found for entity '{entity_name}' in {output_directory}."
        )
    return pd.read_csv(snapshots[0])