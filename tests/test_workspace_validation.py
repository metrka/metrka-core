"""Behavioural tests for read-only dataset workspace validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.datasets.scaffolding import initialize_workspace
from metrka_core.pipeline.workspace_validation import validate_workspace


def test_validate_workspace_checks_bronze_ready_scaffold_without_writing(tmp_path: Path) -> None:
    initialized = initialize_workspace(
        "example_dataset",
        download_url="https://example.org/source.csv",
        workspaces_config_path=tmp_path / "workspaces.local.yaml",
        workspace_root=tmp_path / "datasets" / "example_dataset",
    )
    before = _snapshot(initialized.workspace_root)

    result = validate_workspace(
        "example_dataset", workspaces_config_path=initialized.workspaces_config_path
    )

    assert result.workspace_name == "example_dataset"
    assert result.workspace_root == initialized.workspace_root
    assert result.definition_root == initialized.definition_root
    assert result.data_root == initialized.data_root
    assert result.config_path == initialized.main_config_path
    assert result.quality_config_path == initialized.quality_config_path
    assert result.extractor == "http.files"
    assert result.stream_names == ("data",)
    assert result.pipeline_actions == ("bronze.ingest",)
    assert result.stream_count == 1
    assert result.action_count == 1
    assert result.quality_check_count == 0
    assert result.silver_contract_paths == ()
    assert result.silver_contract_count == 0
    assert _snapshot(initialized.workspace_root) == before


def test_validate_workspace_rejects_an_unregistered_action(tmp_path: Path) -> None:
    initialized = initialize_workspace(
        "example_dataset",
        download_url="https://example.org/source.csv",
        workspaces_config_path=tmp_path / "workspaces.local.yaml",
        workspace_root=tmp_path / "datasets" / "example_dataset",
    )
    main_config = _read_yaml(initialized.main_config_path)
    pipeline = main_config["pipeline"]
    assert isinstance(pipeline, dict)
    pipeline["steps"] = [{"action": "unknown.action"}]
    initialized.main_config_path.write_text(
        yaml.safe_dump(main_config, sort_keys=False), encoding="utf-8"
    )

    with pytest.raises(KeyError, match="Unknown pipeline action 'unknown.action'"):
        validate_workspace(
            "example_dataset", workspaces_config_path=initialized.workspaces_config_path
        )


def test_validate_workspace_checks_contracts_when_silver_is_enabled(tmp_path: Path) -> None:
    initialized = initialize_workspace(
        "example_dataset",
        download_url="https://example.org/source.csv",
        workspaces_config_path=tmp_path / "workspaces.local.yaml",
        workspace_root=tmp_path / "datasets" / "example_dataset",
    )
    main_config = _read_yaml(initialized.main_config_path)
    streams = main_config["streams"]
    pipeline = main_config["pipeline"]
    assert isinstance(streams, dict)
    assert isinstance(pipeline, dict)
    stream = streams["data"]
    assert isinstance(stream, dict)
    stream["yaml_contract_name"] = "source.yaml"
    stream["silver"] = {
        "partition_by": "version_period",
        "version_period": {"strategy": "max_column", "grain": "year", "column": "year"},
        "input": {"format": "csv", "options": {}},
        "outputs": ["csv"],
    }
    pipeline["steps"] = [{"action": "bronze.ingest"}, {"action": "silver.process"}]
    initialized.main_config_path.write_text(
        yaml.safe_dump(main_config, sort_keys=False), encoding="utf-8"
    )
    (initialized.definition_root / "conf" / "source.yaml").write_text(
        "tables: {}\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="missing or empty 'tables' mapping"):
        validate_workspace(
            "example_dataset", workspaces_config_path=initialized.workspaces_config_path
        )


def _snapshot(root: Path) -> dict[str, bytes | None]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes() if path.is_file() else None
        for path in sorted(root.rglob("*"))
    }


def _read_yaml(path: Path) -> dict[str, object]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value
