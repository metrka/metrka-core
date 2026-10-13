"""
Small helpers for loading YAML pipeline configs.

We read UTF-8 YAML, validate the shape we expect and raise errors that tell you what's
missing (and where).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from metrka_core.transform.profiles import expand_profiles

logger = logging.getLogger(__name__)


class ConfigError(ValueError):
    """YAML loaded fine, but the config shape isn't what the pipeline expects."""


class PipelineConfigError(ConfigError):
    """Pipeline YAML has an invalid structure."""


class RuntimeConfigError(ConfigError):
    """Runtime environment configuration is invalid."""


class QualitySettings(BaseModel):
    """Reference to the quality configuration used by a pipeline."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    config: str = Field(min_length=1, strict=True)


class RuntimeEnvironment(StrEnum):
    """Supported Metrka runtime environments."""

    DEVELOPMENT = "development"
    PRODUCTION = "production"


def parse_quality_settings(raw: object) -> QualitySettings:
    """Validate the pipeline.quality configuration block."""

    try:
        return QualitySettings.model_validate(raw)
    except ValidationError as exc:
        raise PipelineConfigError(f"Invalid pipeline.quality configuration: {exc}") from exc


def resolve_runtime_environment(raw: str | None) -> RuntimeEnvironment:
    """Resolve and validate METRKA_ENV."""

    normalized = (raw if raw is not None else RuntimeEnvironment.DEVELOPMENT.value).strip().lower()

    try:
        return RuntimeEnvironment(normalized)
    except ValueError as exc:
        raise RuntimeConfigError(
            f"METRKA_ENV must be 'development' or 'production', received {normalized!r}"
        ) from exc


def load_yaml(cfg_path: str | Path) -> dict[str, Any]:
    """Load a UTF-8 YAML file and return the root mapping."""

    if not isinstance(cfg_path, (str, Path)):
        raise TypeError(f"cfg_path must be str or pathlib.Path, got {type(cfg_path)!r}")

    cfg_path = Path(cfg_path)

    logger.debug("start: path=%s", cfg_path)

    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("failed: file not found path = %s", cfg_path, exc_info=True)
        raise
    except yaml.YAMLError:
        logger.error("failed: invalid YAML: path=%s", cfg_path, exc_info=True)
        raise

    if data is None:
        logger.error("failed: empty config path=%s", cfg_path)
        raise ConfigError(f"Config is empty: {cfg_path}")

    if not isinstance(data, Mapping):
        logger.error("failed: root not a mapping type=%s path=%s", type(data).__name__, cfg_path)
        raise ConfigError(
            f"Config root must be a mapping/dict, got {type(data).__name__}: {cfg_path}"
        )
    out = dict(data)
    logger.debug("keys=%d path=%s", len(out), cfg_path)
    logger.debug("top-level keys=%s", sorted(map(str, out.keys())))

    return out


def require_mapping(obj: Any, *, where: str) -> dict[str, Any]:
    """Return obj as a dict or raise ConfigError with a useful 'where' message."""

    if isinstance(obj, dict):
        logger.debug("ok: where=%s type=dict", where)
        return obj

    if isinstance(obj, Mapping):
        logger.debug("ok: where=%s type=%s (coerced to dict)", where, type(obj).__name__)
        return dict(obj)

    raise ConfigError(f"Expected a mapping at {where}, got {type(obj).__name__}")


def load_table_cfg(cfg_path: str | Path, *, table_key: str) -> dict[str, Any]:
    """Load YAML config and return cfg['tables'][table_key] as a dict (with helpful errors)."""

    if not isinstance(table_key, str) or not table_key.strip():
        raise ValueError("table_key must be non-empty string")

    cfg_path = Path(cfg_path)
    logger.debug("start: path=%s table_key=%s", cfg_path, table_key)

    cfg = expand_profiles(load_yaml(cfg_path), contract_path=cfg_path)

    if "tables" not in cfg:
        logger.error("failed: missing 'tables' path=%s", cfg_path)
        raise KeyError(f"Missing required key 'tables' in config: {cfg_path}")

    tables = require_mapping(cfg["tables"], where="cfg['tables']")

    if table_key not in tables:
        available = ", ".join(sorted(map(str, tables.keys())))
        logger.error(
            "failed: unknown table_key=%s path=%s available=[%s]", table_key, cfg_path, available
        )
        raise KeyError(
            f"Unknown table_key={table_key!r} in config: {cfg_path}. Available keys: [{available}]"
        )

    table_cfg = require_mapping(tables[table_key], where=f"cfg['tables'][{table_key!r}]")

    logger.info("done: path=%s table_key=%s keys=%d", cfg_path, table_key, len(table_cfg))
    logger.debug("table_cfg keys=%s", sorted(map(str, table_cfg.keys())))

    return table_cfg
