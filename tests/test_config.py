"""
Tests for pipeline.config.

Covers `load_yaml()` and `load_table_cfg()` happy paths and the common failure cases.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.pipeline.config import ConfigError, load_table_cfg, load_yaml


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_load_yaml_valid_mapping(tmp_path: Path) -> None:
    """Valid YAML mapping loads into a dict."""
    cfg = tmp_path / "conf.yml"
    _write_text(cfg, "a: 1\nb:\n c: 2\n")
    out = load_yaml(cfg)
    assert out["a"] == 1
    assert out["b"]["c"] == 2


def test_load_yaml_accepts_str_path(tmp_path: Path) -> None:
    """`load_yaml()` accepts str paths too."""
    cfg = tmp_path / "conf.yml"
    _write_text(cfg, "a: 1\n")
    out = load_yaml(str(cfg))
    assert out["a"] == 1


def test_load_yaml_empty_file_raises_configerror(tmp_path: Path) -> None:
    """Empty YAML file raises ConfigError."""
    cfg = tmp_path / "empty.yml"
    _write_text(cfg, "")
    with pytest.raises(ConfigError):
        load_yaml(cfg)


def test_load_yaml_root_not_mapping_raises_configerror(tmp_path: Path) -> None:
    """Non-mapping root raises ConfigError."""
    cfg = tmp_path / "list.yml"
    _write_text(cfg, "- a\n- b\n")
    with pytest.raises(ConfigError):
        load_yaml(cfg)


def test_load_yaml_invalid_yaml_raises_yaml_error(tmp_path: Path) -> None:
    """Bad YAML syntax raises yaml.YAMLError."""
    cfg = tmp_path / "bad.yml"
    _write_text(cfg, "a: [1, 2\n")
    with pytest.raises(yaml.YAMLError):
        load_yaml(cfg)


def test_load_yaml_missing_file_raises_filenotfound(tmp_path: Path) -> None:
    """Missing file raises FileNotFoundError."""
    missing = tmp_path / "nope.yml"
    with pytest.raises(FileNotFoundError):
        load_yaml(missing)


def test_load_table_cfg_success(tmp_path: Path) -> None:
    """Returns the table config for a known key."""
    cfg = tmp_path / "conf.yml"
    _write_text(
        cfg,
        """
 tables:
  active_inmates:
   source: foo
   columns: [a, b]
  """.lstrip(),
    )

    table_cfg = load_table_cfg(cfg, table_key="active_inmates")
    assert table_cfg["source"] == "foo"
    assert table_cfg["columns"] == ["a", "b"]


def test_load_table_cfg_missing_tables_key(tmp_path: Path) -> None:
    """Missing 'tables' raises KeyError."""

    cfg = tmp_path / "conf.yml"
    _write_text(cfg, "x: 1\n")
    with pytest.raises(KeyError):
        load_table_cfg(cfg, table_key="active_inmates")


def test_load_table_cfg_tables_not_mapping(tmp_path: Path) -> None:
    """tables must be a mapping."""
    cfg = tmp_path / "conf.yml"
    _write_text(
        cfg,
        """
  tables:
    - a
    - b
""".lstrip(),
    )
    with pytest.raises(ConfigError):
        load_table_cfg(cfg, table_key="active_inmates")


def test_load_table_cfg_unknown_table_key_lists_available(tmp_path: Path) -> None:
    """Unknown key raises and lists available keys."""
    cfg = tmp_path / "conf.yml"
    _write_text(
        cfg,
        """
  tables:
    one: {a: 1}
    two: {a: 2}
    """.lstrip(),
    )
    with pytest.raises(KeyError) as e:
        load_table_cfg(cfg, table_key="missing")

    msg = str(e.value)
    assert "Available keys" in msg
    assert "one" in msg and "two" in msg


def test_load_table_cfg_table_block_not_mapping(tmp_path: Path) -> None:
    """Selected table block must be a mapping."""
    cfg = tmp_path / "conf.yml"
    _write_text(
        cfg,
        """
  tables:
    active_inmates:
      - a
      - b
  """.lstrip(),
    )
    with pytest.raises(ConfigError):
        load_table_cfg(cfg, table_key="active_inmates")


def test_load_table_cfg_rejects_empty_table_key(tmp_path: Path) -> None:
    """Empty `table_key` is rejected."""
    cfg = tmp_path / "conf.yml"
    _write_text(cfg, "tables:\n  a: \n    x: 1\n")
    with pytest.raises(ValueError):
        load_table_cfg(cfg, table_key="")
