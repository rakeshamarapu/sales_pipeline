"""Capture and persist structured pipeline execution metrics."""

import json
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.utils.config import load_config, resolve_project_path

_WRITE_LOCK = threading.Lock()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _log_directory(output_directory: str | Path | None) -> Path:
    if output_directory is None:
        configured_path = load_config().get("paths", {}).get("logs", "logs")
        return resolve_project_path(configured_path)
    path = Path(output_directory)
    return path if path.is_absolute() else resolve_project_path(path)


@dataclass
class PipelineRun:
    """Counts, timestamps, status, and errors for one pipeline execution."""

    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    start_time: datetime = field(default_factory=_utc_now)
    end_time: datetime | None = None
    source_record_count: int = 0
    raw_record_count: int = 0
    clean_record_count: int = 0
    curated_record_count: int = 0
    rejected_record_count: int = 0
    status: str = "RUNNING"
    errors: list[str] = field(default_factory=list)

    def finish(self, success: bool, error: BaseException | str | None = None) -> None:
        """Set completion time/status and retain a human-readable failure message."""
        self.end_time = _utc_now()
        self.status = "SUCCESS" if success else "FAILED"
        if error is not None:
            self.errors.append(str(error))

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-ready execution metrics."""
        metrics = asdict(self)
        metrics["start_time"] = self.start_time.isoformat()
        metrics["end_time"] = self.end_time.isoformat() if self.end_time else None
        return metrics


def write_execution_log(
    run: PipelineRun,
    output_directory: str | Path | None = None,
) -> Path:
    """Append one JSON execution record to the configured logs directory."""
    directory = _log_directory(output_directory)
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "pipeline_executions.jsonl"
    serialized_run = json.dumps(run.to_dict(), sort_keys=True)
    with _WRITE_LOCK, log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(serialized_run + "\n")
    return log_path