from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.quality.config import load_quality_config
from metrka_core.transform.validation import ContractValidationError, validate_contract_file
from metrka_core.utils.yaml_loading import safe_load


def test_reads_yaml_like_safe_load() -> None:
    text = "base: &base\n  a: 1\nmerged:\n  <<: *base\n  b: 2\nlist: [1, 2]\n"

    assert safe_load(text) == yaml.safe_load(text)


def test_merged_key_may_be_overridden() -> None:
    assert safe_load("base: &base\n  a: 1\nmerged:\n  <<: *base\n  a: 2\n")["merged"] == {"a": 2}


def test_rejects_repeated_key_in_nested_mapping() -> None:
    text = "meta:\n  methodology:\n    rules:\n    rules:\n      x: 1\n"

    with pytest.raises(yaml.YAMLError, match="found duplicate key 'rules'") as raised:
        safe_load(text)

    assert "line 4" in str(raised.value)


def test_contract_validation_rejects_repeated_key(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(
        "tables:\n"
        "  beds:\n"
        "    columns:\n"
        "      County: {rename_to: geography_name, cast_to: string}\n"
        "  beds:\n"
        "    columns:\n"
        "      County: {rename_to: geography_name, cast_to: string}\n",
        encoding="utf-8",
    )

    with pytest.raises(ContractValidationError, match="found duplicate key 'beds'"):
        validate_contract_file(contract_path)


def test_quality_config_rejects_repeated_key(tmp_path: Path) -> None:
    quality_path = tmp_path / "quality.yaml"
    quality_path.write_text("version: 1\nversion: 1\ntables: {}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Invalid quality YAML"):
        load_quality_config(quality_path)
