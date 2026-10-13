"""Tests for the public ``dataset_id`` identity rule shared with Metrka readers."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.api import DatasetIdentityError, parse_dataset_id
from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.datasets.yaml_workspace_resolver import YamlWorkspaceLocationResolver


@pytest.mark.parametrize(
    ("dataset_id", "workspace_name", "stream_name"),
    [
        ("demo.records", "demo", "records"),
        ("agency.region.records", "agency.region", "records"),
        (
            "fl_healthcharts.adult_substance_abuse_beds",
            "fl_healthcharts",
            "adult_substance_abuse_beds",
        ),
    ],
)
def test_parses_workspace_and_stream_from_dataset_id(
    dataset_id: str, workspace_name: str, stream_name: str
) -> None:
    identity = parse_dataset_id(dataset_id)

    assert identity.dataset_id == dataset_id
    assert identity.workspace_name == workspace_name
    assert identity.stream_name == stream_name


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, "must be a non-empty string"),
        ("", "must be a non-empty string"),
        ("   ", "must be a non-empty string"),
        (" demo.records", "surrounding whitespace"),
        ("demo.records ", "surrounding whitespace"),
        ("demo", "does not contain a stream name"),
        (".records", "does not contain both identifiers"),
        ("demo.", "does not contain both identifiers"),
        ("demo/region.records", "contains a path separator"),
        ("demo.records/archive", "contains a path separator"),
        (r"demo.records\archive", "contains a path separator"),
    ],
)
def test_rejects_noncanonical_dataset_id(value: object, reason: str) -> None:
    with pytest.raises(DatasetIdentityError) as captured:
        parse_dataset_id(value)

    assert reason in captured.value.reason
    assert isinstance(captured.value, ValueError)


@pytest.mark.parametrize("workspace_name", ["demo", "agency.region", "fl_healthcharts"])
def test_parse_returns_the_parts_the_pipeline_joined(workspace_name: str) -> None:
    config = SourceConfig(
        workspace_name=workspace_name,
        streams={"records": StreamConfig(name="records", official_filename="records.csv")},
    )

    identity = parse_dataset_id(config.dataset_id("records"))

    assert identity.workspace_name == workspace_name
    assert identity.stream_name == "records"


def test_resolve_dataset_rejects_a_malformed_dataset_id(tmp_path: Path) -> None:
    registry = tmp_path / "workspaces.local.yaml"
    registry.write_text(yaml.safe_dump({"schema_version": 1, "workspaces": {}}), encoding="utf-8")
    resolver = YamlWorkspaceLocationResolver.from_config_path(registry)

    with pytest.raises(DatasetIdentityError, match="path separator"):
        resolver.resolve_dataset("demo/region.records")
