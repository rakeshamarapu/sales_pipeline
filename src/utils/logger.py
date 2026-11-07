"""Configure console and rotating-file logging for the pipeline."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.utils.config import load_config, resolve_project_path

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(
    log_directory: str | Path | None = None,
    level: int = logging.INFO,
) -> Path:
    """Set up idempotent console and rotating file handlers; return the log path."""
    if log_directory is None:
        configured_path = load_config().get("paths", {}).get("logs", "logs")
        output_directory = resolve_project_path(configured_path)
    else:
        output_directory = Path(log_directory)
        if not output_directory.is_absolute():
            output_directory = resolve_project_path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    log_path = output_directory / "pipeline.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
    for handler in list(root_logger.handlers):
        previous_log_path = getattr(handler, "_retail_log_path", None)
        if previous_log_path is not None and previous_log_path != str(log_path):
            root_logger.removeHandler(handler)
            handler.close()

    if not any(getattr(handler, "_retail_console", False) for handler in root_logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler._retail_console = True
        root_logger.addHandler(console_handler)

    if not any(
        getattr(handler, "_retail_log_path", None) == str(log_path)
        for handler in root_logger.handlers
    ):
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=5_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler._retail_log_path = str(log_path)
        root_logger.addHandler(file_handler)

    return log_path