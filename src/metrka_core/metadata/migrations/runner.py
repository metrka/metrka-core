"""Programmatic Alembic configuration for metrka-core."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config

MIGRATION_ROOT = Path(__file__).resolve().parent


def build_alembic_config(*, conninfo: str | None = None) -> Config:
    """Build an Alembic configuration without storing credentials."""

    config = Config()

    config.set_main_option("script_location", MIGRATION_ROOT.as_posix())

    if conninfo is not None:
        config.attributes["metadata_conninfo"] = conninfo

    return config
