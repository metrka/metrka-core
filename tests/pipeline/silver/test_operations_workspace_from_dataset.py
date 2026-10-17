"""Operations find a dataset's folder from its ID, without a repeated workspace name."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from metrka_core.datasets.yaml_workspace_resolver import YamlWorkspaceLocationResolver
from metrka_core.pipeline.silver.manage_publication_candidates import (
    _build_parser,
    candidate_location,
)
from metrka_core.pipeline.silver.reconcile_publications import reconciliation_location


def _resolver(tmp_path: Path) -> YamlWorkspaceLocationResolver:
    (tmp_path / "flat").mkdir()
    (tmp_path / "fl" / "beds").mkdir(parents=True)
    (tmp_path / "mfl" / "beds").mkdir(parents=True)
    (tmp_path / "store" / "mfl").mkdir(parents=True)
    registry = tmp_path / "workspaces.local.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "workspaces": {
                    "flat": {"placement": "portable", "workspace_root": str(tmp_path / "flat")},
                    "fl": {
                        "placement": "portable",
                        "workspace_root": str(tmp_path / "fl"),
                        "layout": "dataset_folders",
                    },
                    "mfl": {
                        "placement": "managed",
                        "definition_root": str(tmp_path / "mfl"),
                        "data_root": str(tmp_path / "store" / "mfl"),
                        "layout": "dataset_folders",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return YamlWorkspaceLocationResolver.from_config_path(registry)


@pytest.mark.parametrize(
    ("dataset_id", "folder", "data"),
    [
        ("flat.records", "flat", "flat/data"),
        ("fl.beds", "fl/beds", "fl/beds/data"),
        ("mfl.beds", "mfl/beds", "store/mfl/beds"),
    ],
)
def test_candidate_dataset_names_its_folder(
    tmp_path: Path, dataset_id: str, folder: str, data: str
) -> None:
    location = candidate_location(_resolver(tmp_path), dataset_id=dataset_id, workspace=None)

    assert location.definition_root == (tmp_path / folder).resolve()
    assert location.data_root == (tmp_path / data).resolve()


@pytest.mark.parametrize(
    ("dataset_id", "workspace"), [("flat.records", "flat"), ("fl.beds", "fl.beds")]
)
def test_explicit_workspace_may_repeat_the_candidates_own(
    tmp_path: Path, dataset_id: str, workspace: str
) -> None:
    location = candidate_location(_resolver(tmp_path), dataset_id=dataset_id, workspace=workspace)

    assert location.name == workspace


def test_explicit_workspace_of_another_dataset_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError) as raised:
        candidate_location(_resolver(tmp_path), dataset_id="fl.beds", workspace="flat")

    assert "belongs to workspace 'fl.beds', not 'flat'" in str(raised.value)


def test_publish_needs_only_the_candidate_id() -> None:
    args = _build_parser(prog="metrka").parse_args(["publish", "candidate-1"])

    assert args.candidate_id == "candidate-1"
    assert args.workspace is None


def test_reconciliation_finds_the_folder_of_its_datasets(tmp_path: Path) -> None:
    location = reconciliation_location(_resolver(tmp_path), dataset_ids=["fl.beds"], workspace=None)

    assert location.name == "fl.beds"


def test_reconciliation_runs_one_workspace_at_a_time(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="different workspaces"):
        reconciliation_location(
            _resolver(tmp_path), dataset_ids=["fl.beds", "flat.records"], workspace=None
        )


def test_reconciliation_rejects_a_workspace_of_other_datasets(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="belong to workspace 'fl.beds', not 'flat'"):
        reconciliation_location(_resolver(tmp_path), dataset_ids=["fl.beds"], workspace="flat")


def test_orphan_audit_without_datasets_names_its_workspace(tmp_path: Path) -> None:
    resolver = _resolver(tmp_path)

    assert reconciliation_location(resolver, dataset_ids=[], workspace="flat").name == "flat"
    with pytest.raises(RuntimeError, match="needs --workspace"):
        reconciliation_location(resolver, dataset_ids=[], workspace=None)
