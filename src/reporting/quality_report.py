"""Build and write concise data-quality reports from Phase 3 results."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.config import load_config, resolve_project_path
from src.validation.schema_validator import ValidationResult


def build_quality_report(
    entity: str,
    validation: ValidationResult,
    *,
    source_record_count: int | None = None,
    duplicate_count: int | None = None,
) -> dict[str, Any]:
    """Summarize input, valid, duplicate, missing, and rejected row counts."""
    total_records = validation.total_records
    duplicate_records = (
        len(validation.duplicate_records)
        if duplicate_count is None
        else duplicate_count
    )
    invalid_count = len(validation.invalid_records)
    return {
        "entity": entity,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_records_received": total_records,
        "source_record_count": (
            total_records if source_record_count is None else source_record_count
        ),
        "valid_records": len(validation.valid_records),
        "invalid_records": invalid_count,
        "duplicate_records": duplicate_records,
        "missing_values": validation.missing_values,
        "rejected_records": invalid_count,
    }


def write_quality_report(
    reports: dict[str, dict[str, Any]],
    *,
    run_id: str,
    output_directory: str | Path | None = None,
) -> Path:
    """Write entity quality summaries as a JSON report in the logs directory."""
    if output_directory is None:
        configured_path = load_config().get("paths", {}).get("logs", "logs")
        directory = resolve_project_path(configured_path)
    else:
        directory = Path(output_directory)
        if not directory.is_absolute():
            directory = resolve_project_path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / f"quality_report_{run_id}.json"
    output_path.write_text(
        json.dumps(reports, indent=2, sort_keys=True), encoding="utf-8"
    )
    return output_path