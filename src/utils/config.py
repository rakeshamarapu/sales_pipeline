"""Load project settings and resolve project-relative paths."""

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def get_project_root() -> Path:
    """Return the repository root, independent of the current directory."""
    return Path(__file__).resolve().parents[2]


def resolve_project_path(path: str | Path) -> Path:
    """Resolve a configured path relative to the repository root."""
    resolved_path = Path(path)
    if resolved_path.is_absolute():
        return resolved_path
    return get_project_root() / resolved_path


def load_project_environment() -> None:
    """Load local environment values without overriding existing variables."""
    load_dotenv(get_project_root() / ".env")


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Read the YAML configuration, defaulting to config/config.yaml."""
    path = (
        Path(config_path)
        if config_path is not None
        else get_project_root() / "config" / "config.yaml"
    )
    if not path.is_absolute():
        path = get_project_root() / path

    with path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError(f"Configuration file must contain a YAML mapping: {path}")
    return config