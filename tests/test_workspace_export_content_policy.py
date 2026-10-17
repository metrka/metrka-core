"""Security boundary for customer-visible workspace definitions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZipFile

import pytest

from metrka_core.datasets.workspace_export import (
    WorkspaceExportContentPolicyError,
    export_workspace,
)
from metrka_core.datasets.workspace_location import WorkspaceLocation


@dataclass(frozen=True, slots=True)
class _FrozenClock:
    def now_utc(self) -> datetime:
        return datetime(2026, 8, 20, 12, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class _StaticWorkspaceLocationResolver:
    location: WorkspaceLocation

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        assert workspace_name == self.location.workspace_name
        return self.location


def _write(path: Path, content: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8")
    else:
        path.write_bytes(content)


def _export(location: WorkspaceLocation, destination: Path, *, overwrite: bool = False) -> None:
    export_workspace(
        location.workspace_name,
        destination,
        workspace_location_resolver=_StaticWorkspaceLocationResolver(location),
        clock=_FrozenClock(),
        overwrite=overwrite,
    )


def test_export_reports_all_sensitive_definition_files_before_writing_zip(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", "workspace: example\n")
    _write(
        workspace_root / "conf" / "metadata.yaml",
        "runtime:\n  metadata_database:\n    host: internal-db\n    user: metrka\n",
    )
    _write(workspace_root / "conf" / "customer-credentials.json", '{"client": "demo"}\n')
    _write(workspace_root / ".env.production", "METRKA_METADATA_PASSWORD=do-not-export\n")
    _write(
        workspace_root / "keys" / "signing.pem",
        "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n",
    )
    _write(workspace_root / "data" / "files" / "silver" / "table.csv", "id\n1\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)
    destination = tmp_path / "example.zip"
    directory_before_export = {path.name for path in destination.parent.iterdir()}
    expected_paths = {
        ".env.production",
        "conf/customer-credentials.json",
        "conf/metadata.yaml",
        "keys/signing.pem",
    }

    with pytest.raises(WorkspaceExportContentPolicyError) as captured:
        _export(location, destination)

    assert {violation.path for violation in captured.value.violations} == expected_paths
    message = str(captured.value)
    assert "definition_root is customer-visible" in message
    assert "metadata_database" in message
    assert str(workspace_root) not in message

    for path in expected_paths:
        assert f"- {path}:" in message

    assert not destination.exists()
    assert {path.name for path in destination.parent.iterdir()} == directory_before_export
    assert list(destination.parent.glob(".example.*.tmp.zip")) == []


def test_export_policy_failure_preserves_existing_verified_zip(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", "workspace: example\n")
    _write(workspace_root / "data" / "files" / "silver" / "table.csv", "id\n1\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)
    destination = tmp_path / "exports" / "example.zip"

    _export(location, destination)
    package_before_failure = destination.read_bytes()
    directory_before_failure = {path.name for path in destination.parent.iterdir()}

    _write(
        workspace_root / "conf" / "deployment.yaml",
        "metadata_database:\n  host: internal-db\n  user: metrka\n",
    )

    with pytest.raises(WorkspaceExportContentPolicyError) as captured:
        _export(location, destination, overwrite=True)

    assert [violation.path for violation in captured.value.violations] == ["conf/deployment.yaml"]
    assert destination.read_bytes() == package_before_failure
    assert {path.name for path in destination.parent.iterdir()} == directory_before_failure
    assert list(destination.parent.glob(".example.*.tmp.zip")) == []

    with ZipFile(destination, "r") as archive:
        assert "example/metrka-workspace-manifest.json" in archive.namelist()


def test_export_keeps_safe_quality_documentation_and_pipeline_definitions(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", "workspace: example\n")
    _write(workspace_root / "conf" / "quality.yaml", "quality_checks: []\n")
    _write(workspace_root / "conf" / "models" / "wide-table.yaml", "dimensions: [year]\n")
    _write(workspace_root / "docs" / "secrets-handling.md", "# Deployment guidance\n")
    _write(workspace_root / "data" / "files" / "silver" / "table.csv", "id\n1\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)
    destination = tmp_path / "example.zip"

    _export(location, destination)

    with ZipFile(destination, "r") as archive:
        assert set(archive.namelist()) == {
            "example/conf/main.yaml",
            "example/conf/models/wide-table.yaml",
            "example/conf/quality.yaml",
            "example/data/files/silver/table.csv",
            "example/docs/secrets-handling.md",
            "example/metrka-workspace-manifest.json",
        }


def test_export_fails_closed_when_structured_definition_cannot_be_inspected(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", "workspace: example\n")
    _write(workspace_root / "conf" / "broken.yaml", "unfinished: [\n")
    _write(workspace_root / "data" / "files" / "silver" / "table.csv", "id\n1\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)
    destination = tmp_path / "example.zip"

    with pytest.raises(WorkspaceExportContentPolicyError) as captured:
        _export(location, destination)

    assert [violation.path for violation in captured.value.violations] == ["conf/broken.yaml"]
    assert "cannot be inspected safely" in str(captured.value)
    assert not destination.exists()
