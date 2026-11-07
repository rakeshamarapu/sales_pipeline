"""Create SQLAlchemy engines using credentials from environment variables."""

import os
from pathlib import Path

from sqlalchemy import URL, create_engine
from sqlalchemy.engine import Engine

from src.utils.config import load_config, load_project_environment


class DatabaseConfigurationError(ValueError):
    """Raised when required PostgreSQL environment settings are missing."""


def create_database_engine(config_path: str | Path | None = None) -> Engine:
    """Build a PostgreSQL engine without embedding credentials in source code."""
    load_project_environment()
    database_config = load_config(config_path).get("database", {})

    required_settings = {
        "host": database_config.get("host_env", "DB_HOST"),
        "database": database_config.get("name_env", "DB_NAME"),
        "username": database_config.get("user_env", "DB_USER"),
        "password": database_config.get("password_env", "DB_PASSWORD"),
    }
    missing_settings = [
        variable_name
        for variable_name in required_settings.values()
        if not os.getenv(variable_name)
    ]
    if missing_settings:
        missing = ", ".join(missing_settings)
        raise DatabaseConfigurationError(
            f"Missing PostgreSQL environment variables: {missing}. "
            "Set them in .env or the process environment."
        )

    port_variable = database_config.get("port_env", "DB_PORT")
    try:
        port = int(os.getenv(port_variable, database_config.get("default_port", 5432)))
    except (TypeError, ValueError) as error:
        raise DatabaseConfigurationError(
            f"PostgreSQL port in {port_variable} must be an integer."
        ) from error

    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ[required_settings["username"]],
        password=os.environ[required_settings["password"]],
        host=os.environ[required_settings["host"]],
        port=port,
        database=os.environ[required_settings["database"]],
    )
    return create_engine(connection_url, pool_pre_ping=True)