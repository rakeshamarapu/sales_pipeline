"""Compare source, clean, rejected, and duplicate counts by entity."""

from pathlib import Path

import pandas as pd

from src.utils.config import load_config, resolve_project_path


def build_reconciliation_report(
    source_counts: dict[str, int],
    target_counts: dict[str, int],
    *,
    rejected_counts: dict[str, int] | None = None,
    duplicate_counts: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Return a count reconciliation; duplicate_counts means rows removed by dedupe."""
    rejected_counts = rejected_counts or {}
    duplicate_counts = duplicate_counts or {}
    entities = sorted(set(source_counts) | set(target_counts))
    rows = []
    for entity in entities:
        source_count = source_counts.get(entity, 0)
        target_count = target_counts.get(entity, 0)
        rejected_count = rejected_counts.get(entity, 0)
        duplicates_removed = duplicate_counts.get(entity, 0)
        accounted_count = target_count + rejected_count + duplicates_removed
        rows.append(
            {
                "entity": entity,
                "source_count": source_count,
                "target_count": target_count,
                "rejected_count": rejected_count,
                "duplicates_removed": duplicates_removed,
                "unaccounted_count": source_count - accounted_count,
                "status": "MATCH" if source_count == accounted_count else "MISMATCH",
            }
        )
    return pd.DataFrame(rows)


def write_reconciliation_report(
    report: pd.DataFrame,
    *,
    run_id: str,
    output_directory: str | Path | None = None,
) -> Path:
    """Write a reconciliation data frame to a timestampable CSV report."""
    if output_directory is None:
        configured_path = load_config().get("paths", {}).get("logs", "logs")
        directory = resolve_project_path(configured_path)
    else:
        directory = Path(output_directory)
        if not directory.is_absolute():
            directory = resolve_project_path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / f"reconciliation_{run_id}.csv"
    report.to_csv(output_path, index=False, encoding="utf-8")
    return output_path