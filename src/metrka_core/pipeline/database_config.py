"""Resolve PostgreSQL configuration at the application boundary."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from psycopg.conninfo import make_conninfo

logger = logging.getLogger(__name__)


def _conninfo_from_yaml(config_path: Path) -> str:
    if not config_path.is_file():
        raise RuntimeError(f"Metadata database config does not exist: {config_path}")

    try:
        raw: Any = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Invalid YAML in {config_path}") from exc

    if not isinstance(raw, dict):
        raise RuntimeError(f"YAML root must be a mapping: {config_path}")

    database = raw.get("metadata_database")

    if not isinstance(database, dict):
        raise RuntimeError(f"{config_path} is missing metadata_database")

    config: dict[str, str] = {}

    for key in ("dbname", "host", "port", "user"):
        value = database.get(key)

        if not isinstance(value, (str, int)) or not str(value).strip():
            raise RuntimeError(f"{config_path} metadata_database.{key} must be set")

        config[key] = str(value).strip()

    password = os.environ.get("METRKA_METADATA_PASSWORD")

    if password:
        config["password"] = password

    logger.debug(
        ("Loaded PostgreSQL metadata target dbname=%s host=%s port=%s user=%s from %s"),
        config["dbname"],
        config["host"],
        config["port"],
        config["user"],
        config_path,
    )

    return make_conninfo("", **config)


def resolve_metadata_conninfo(
    *, conninfo: str | None = None, config_path: str | Path | None = None
) -> str:
    """
    Resolve metadata connection information.

    Explicit arguments take precedence over environment variables.
    """

    if conninfo is not None:
        if not conninfo.strip():
            raise ValueError("Metadata conninfo must not be empty")

        if config_path is not None:
            raise ValueError("Provide metadata conninfo or config_path, not both")

        return conninfo.strip()

    if config_path is not None:
        return _conninfo_from_yaml(Path(config_path).expanduser().resolve())

    environment_conninfo = os.environ.get("METRKA_METADATA_DSN")

    if environment_conninfo:
        return environment_conninfo.strip()

    environment_config_path = os.environ.get("METRKA_METADATA_CONFIG_PATH")

    if environment_config_path:
        return _conninfo_from_yaml(Path(environment_config_path).expanduser().resolve())

    raise RuntimeError(
        "PostgreSQL metadata configuration is missing. "
        "Pass metadata_conninfo or metadata_config_path, "
        "or set METRKA_METADATA_DSN or "
        "METRKA_METADATA_CONFIG_PATH."
    )
