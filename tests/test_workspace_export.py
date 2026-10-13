"""Behavioural contract for portable customer workspace exports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from metrka_core.datasets.workspace_export import (
    WORKSPACE_EXPORT_MANIFEST_NAME,
    WorkspaceExportIntegrityError,
    WorkspaceExportManifest,
    export_workspace,
    verify_workspace_export,
)
from metrka_core.datasets.workspace_export_models import (
    WorkspaceExportFile,
    WorkspaceExportFileRole,
)
from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement


@dataclass(frozen=True, slots=True)
class FrozenClock:
    value: datetime

    def now_utc(self) -> datetime:
        return self.value


@dataclass(frozen=True, slots=True)
class StaticWorkspaceLocationResolver:
    location: WorkspaceLocation

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        assert workspace_name == self.location.workspace_name
        return self.location


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _export(*, location: WorkspaceLocation, destination: Path, overwrite: bool = False):
    return export_workspace(
        location.workspace_name,
        destination,
        workspace_location_resolver=StaticWorkspaceLocationResolver(location),
        clock=FrozenClock(datetime(2026, 8, 19, 12, 30, tzinfo=UTC)),
        overwrite=overwrite,
    )


def _manifest(package_path: Path, workspace_name: str) -> WorkspaceExportManifest:
    with ZipFile(package_path, "r") as archive:
        return WorkspaceExportManifest.from_json_bytes(
            archive.read(f"{workspace_name}/{WORKSPACE_EXPORT_MANIFEST_NAME}")
        )


def test_portable_export_reconstructs_one_workspace_without_duplicating_data(
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "source" / "example_dataset"
    _write(workspace_root / "README.md", b"Example dataset\n")
    _write(workspace_root / ".git" / "config", b"must not leave the repository\n")
    _write(workspace_root / ".pytest_cache" / "README.md", b"cache\n")
    _write(workspace_root / "conf" / "main.yaml", b"workspace: example_dataset\n")
    _write(workspace_root / "pipelines" / "run.py", b"print('run')\n")
    _write(workspace_root / "data" / "files" / "silver" / "data.csv", b"year,value\n")
    _write(workspace_root / "data" / "files" / "silver" / "staging" / "partial.csv", b"x")
    _write(workspace_root / "data" / "contracts" / "contract.json", b"{}\n")
    location = WorkspaceLocation.portable(
        workspace_name="example_dataset", workspace_root=workspace_root
    )
    destination = tmp_path / "exports" / "example_dataset.zip"

    result = _export(location=location, destination=destination)
    verification = verify_workspace_export(destination)

    assert result.workspace_name == "example_dataset"
    assert result.source_placement is WorkspacePlacement.PORTABLE
    assert result.package_path == destination.resolve()
    assert result.package_checksum == verification.package_checksum
    assert result.file_count == 5
    assert verification.file_count == 5

    with ZipFile(destination, "r") as archive:
        names = set(archive.namelist())
        assert names == {
            f"example_dataset/{WORKSPACE_EXPORT_MANIFEST_NAME}",
            "example_dataset/README.md",
            "example_dataset/conf/main.yaml",
            "example_dataset/pipelines/run.py",
            "example_dataset/data/files/silver/data.csv",
            "example_dataset/data/contracts/contract.json",
        }
        manifest_content = archive.read(f"example_dataset/{WORKSPACE_EXPORT_MANIFEST_NAME}").decode(
            "utf-8"
        )

    assert str(workspace_root.resolve()) not in manifest_content
    assert not any(".git" in name or ".pytest_cache" in name for name in names)
    manifest = _manifest(destination, "example_dataset")
    assert manifest.created_at == datetime(2026, 8, 19, 12, 30, tzinfo=UTC)
    assert [entry.path for entry in manifest.files] == sorted(
        entry.path for entry in manifest.files
    )


def test_managed_export_reassembles_detached_roots_as_a_portable_workspace(tmp_path: Path) -> None:
    definition_root = tmp_path / "definitions" / "customer_health"
    data_root = tmp_path / "storage" / "customer_health"
    _write(definition_root / "conf" / "main.yaml", b"workspace: customer_health\n")
    _write(definition_root / "README.md", b"Customer health\n")
    _write(data_root / "files" / "silver" / "table.parquet", b"PAR1")
    _write(data_root / "receipts" / "executions" / "silver.jsonl", b"{}\n")
    location = WorkspaceLocation.managed(
        workspace_name="customer_health", definition_root=definition_root, data_root=data_root
    )
    destination = tmp_path / "exports" / "customer-health.zip"

    result = _export(location=location, destination=destination)

    assert result.source_placement is WorkspacePlacement.MANAGED
    with ZipFile(destination, "r") as archive:
        assert set(archive.namelist()) == {
            f"customer_health/{WORKSPACE_EXPORT_MANIFEST_NAME}",
            "customer_health/README.md",
            "customer_health/conf/main.yaml",
            "customer_health/data/files/silver/table.parquet",
            "customer_health/data/receipts/executions/silver.jsonl",
        }

    manifest = _manifest(destination, "customer_health")
    roles = {entry.path: entry.role.value for entry in manifest.files}
    assert roles["conf/main.yaml"] == "definition"
    assert roles["data/files/silver/table.parquet"] == "data"


def test_verification_rejects_changed_payload_bytes(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", b"workspace: example\n")
    _write(workspace_root / "data" / "files" / "silver" / "data.csv", b"a\n1\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)
    original = tmp_path / "original.zip"
    changed = tmp_path / "changed.zip"
    _export(location=location, destination=original)

    with (
        ZipFile(original, "r") as source,
        ZipFile(changed, "w", compression=ZIP_DEFLATED) as target,
    ):
        for info in source.infolist():
            content = source.read(info)
            if info.filename == "example/data/files/silver/data.csv":
                content = b"a\n2\n"
            target.writestr(info, content)

    with pytest.raises(WorkspaceExportIntegrityError, match="checksum mismatch"):
        verify_workspace_export(changed)


def test_verification_rejects_unrecorded_and_unsafe_members(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", b"workspace: example\n")
    _write(workspace_root / "data" / "files" / "silver" / "data.csv", b"a\n1\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)
    original = tmp_path / "original.zip"
    extra = tmp_path / "extra.zip"
    _export(location=location, destination=original)

    with ZipFile(original, "r") as source, ZipFile(extra, "w", compression=ZIP_DEFLATED) as target:
        for info in source.infolist():
            target.writestr(info, source.read(info))
        target.writestr("example/unrecorded.txt", b"not declared")

    with pytest.raises(WorkspaceExportIntegrityError, match="membership"):
        verify_workspace_export(extra)

    unsafe = tmp_path / "unsafe.zip"
    with ZipFile(unsafe, "w") as archive:
        archive.writestr("../outside.txt", b"unsafe")

    with pytest.raises(WorkspaceExportIntegrityError, match="unsafe"):
        verify_workspace_export(unsafe)


def test_export_refuses_ambiguous_or_destructive_destinations(tmp_path: Path) -> None:
    definition_root = tmp_path / "definitions"
    data_root = tmp_path / "runtime"
    _write(definition_root / "data" / "manual.txt", b"reserved")
    _write(data_root / "manual.txt", b"runtime")
    location = WorkspaceLocation.managed(
        workspace_name="example", definition_root=definition_root, data_root=data_root
    )

    with pytest.raises(ValueError, match="below data"):
        _export(location=location, destination=tmp_path / "collision.zip")

    (definition_root / "data" / "manual.txt").unlink()
    destination = tmp_path / "existing.zip"
    destination.write_bytes(b"existing")
    with pytest.raises(FileExistsError):
        _export(location=location, destination=destination)
    assert destination.read_bytes() == b"existing"

    result = _export(location=location, destination=destination, overwrite=True)
    assert result.package_path == destination.resolve()
    assert verify_workspace_export(destination).workspace_name == "example"

    with pytest.raises(ValueError, match="outside the source workspace"):
        _export(location=location, destination=data_root / "export.zip")


def test_manifest_rejects_tampered_summary_fields(tmp_path: Path) -> None:
    manifest = {
        "schema_version": 1,
        "package_type": "metrka.customer-workspace",
        "workspace_name": "example",
        "source_placement": "portable",
        "created_at": "2026-08-19T12:30:00+00:00",
        "file_count": 1,
        "total_size_bytes": 0,
        "files": [],
    }
    package_path = tmp_path / "summary.zip"
    with ZipFile(package_path, "w") as archive:
        archive.writestr(
            f"example/{WORKSPACE_EXPORT_MANIFEST_NAME}", json.dumps(manifest).encode("utf-8")
        )

    with pytest.raises(WorkspaceExportIntegrityError, match="file_count"):
        verify_workspace_export(package_path)


def test_export_refuses_workspace_with_active_pipeline_marker(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    _write(workspace_root / "conf" / "main.yaml", b"workspace: example\n")
    _write(workspace_root / "data" / "files" / "silver" / "data.csv", b"a\n1\n")
    marker = workspace_root / "data" / "current" / "latest" / "silver" / "_run_in_progress.json"
    _write(marker, b"{}\n")
    location = WorkspaceLocation.portable(workspace_name="example", workspace_root=workspace_root)

    with pytest.raises(RuntimeError, match="quiescent workspace"):
        _export(location=location, destination=tmp_path / "example.zip")


@pytest.mark.parametrize("path", ["data/CON.txt", "data/report?.csv", "data/name."])
def test_manifest_rejects_paths_that_are_not_cross_platform(path: str) -> None:
    with pytest.raises(ValueError, match="cross-platform|reserved filename"):
        WorkspaceExportFile(
            path=path,
            role=WorkspaceExportFileRole.DATA,
            size_bytes=0,
            checksum="sha256:" + "0" * 64,
        )
