"""Behavioural contract for installing customer workspace packages."""

from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import yaml

import metrka_core.datasets.workspace_import as workspace_import
import metrka_core.datasets.workspace_registry as workspace_registry
from metrka_core.datasets.workspace_export import export_workspace
from metrka_core.datasets.workspace_export_models import WORKSPACE_EXPORT_MANIFEST_NAME
from metrka_core.datasets.workspace_import import import_workspace
from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement
from metrka_core.datasets.yaml_workspace_resolver import YamlWorkspaceLocationResolver
from metrka_core.pipeline.config import RuntimeConfigError


@pytest.fixture
def exported_workspace(tmp_path: Path) -> Path:
    source_root = tmp_path / "source" / "example_workspace"
    _write(source_root / "conf" / "main.yaml", b"workspace_name: example_workspace\n")
    _write(source_root / "data" / "files" / "silver" / "table.csv", b"id,value\n1,ok\n")
    package_path = tmp_path / "packages" / "example_workspace.zip"
    export_workspace(
        "example_workspace",
        package_path,
        workspace_location_resolver=StaticWorkspaceLocationResolver(
            WorkspaceLocation.portable(
                workspace_name="example_workspace", workspace_root=source_root
            )
        ),
    )
    return package_path


class StaticWorkspaceLocationResolver:
    def __init__(self, location: WorkspaceLocation) -> None:
        self._location = location

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        if workspace_name != self._location.workspace_name:
            raise KeyError(workspace_name)
        return self._location


def test_import_workspace_installs_and_registers_a_portable_workspace(
    tmp_path: Path, exported_workspace: Path
) -> None:
    config_path = tmp_path / "consumer" / "workspaces.local.yaml"
    destination_directory = tmp_path / "consumer" / "workspaces"

    result = import_workspace(
        exported_workspace,
        destination_directory=destination_directory,
        workspaces_config_path=config_path,
    )

    expected_root = (destination_directory / "example_workspace").resolve()
    assert result.workspace_name == "example_workspace"
    assert result.source_placement is WorkspacePlacement.PORTABLE
    assert result.workspace_root == expected_root
    assert result.definition_root == expected_root
    assert result.data_root == expected_root / "data"
    assert result.workspaces_config_path == config_path.resolve()
    assert result.package_checksum.startswith("sha256:")
    assert result.file_count == 2
    assert (expected_root / WORKSPACE_EXPORT_MANIFEST_NAME).is_file()
    assert (expected_root / "conf" / "main.yaml").read_bytes() == (
        b"workspace_name: example_workspace\n"
    )
    assert (expected_root / "data" / "files" / "silver" / "table.csv").read_bytes() == (
        b"id,value\n1,ok\n"
    )

    location = YamlWorkspaceLocationResolver.from_config_path(config_path).resolve(
        "example_workspace"
    )
    assert location == WorkspaceLocation.portable(
        workspace_name="example_workspace", workspace_root=expected_root
    )
    assert yaml.safe_load(config_path.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "workspaces": {
            "example_workspace": {
                "placement": "portable",
                "workspace_root": "workspaces/example_workspace",
            }
        },
    }


def test_import_workspace_uses_environment_workspace_config_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exported_workspace: Path
) -> None:
    config_path = tmp_path / "consumer" / "configuration" / "workspaces.yaml"
    destination_directory = tmp_path / "consumer" / "workspaces"
    monkeypatch.setenv("METRKA_WORKSPACES_CONFIG_PATH", str(config_path))

    result = import_workspace(exported_workspace, destination_directory=destination_directory)

    assert result.workspaces_config_path == config_path.resolve()
    assert config_path.is_file()


def test_import_workspace_rejects_implicit_production_config_before_installing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exported_workspace: Path
) -> None:
    destination_directory = tmp_path / "consumer" / "workspaces"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("METRKA_ENV", "production")
    monkeypatch.delenv("METRKA_WORKSPACES_CONFIG_PATH", raising=False)

    with pytest.raises(RuntimeConfigError, match="Production workspace configuration"):
        import_workspace(exported_workspace, destination_directory=destination_directory)

    assert not destination_directory.exists()
    assert not (tmp_path / "workspaces.local.yaml").exists()


def test_import_workspace_preserves_managed_source_identity_but_installs_portably(
    tmp_path: Path,
) -> None:
    definition_root = tmp_path / "producer" / "definitions" / "customer_health"
    data_root = tmp_path / "producer" / "runtime" / "customer_health"
    _write(definition_root / "conf" / "main.yaml", b"workspace_name: customer_health\n")
    _write(data_root / "files" / "silver" / "table.csv", b"id\n1\n")
    package_path = tmp_path / "customer_health.zip"
    export_workspace(
        "customer_health",
        package_path,
        workspace_location_resolver=StaticWorkspaceLocationResolver(
            WorkspaceLocation.managed(
                workspace_name="customer_health",
                definition_root=definition_root,
                data_root=data_root,
            )
        ),
    )

    result = import_workspace(
        package_path,
        destination_directory=tmp_path / "consumer" / "workspaces",
        workspaces_config_path=tmp_path / "consumer" / "workspaces.yaml",
    )

    assert result.source_placement is WorkspacePlacement.MANAGED
    assert result.workspace_root == result.definition_root
    assert result.data_root == result.workspace_root / "data"
    assert (result.workspace_root / "conf" / "main.yaml").is_file()
    assert (result.data_root / "files" / "silver" / "table.csv").is_file()


def test_import_workspace_rejects_duplicate_registration_before_extracting(
    tmp_path: Path, exported_workspace: Path
) -> None:
    config_path = tmp_path / "consumer" / "workspaces.yaml"
    existing_root = tmp_path / "existing" / "example_workspace"
    (existing_root / "data").mkdir(parents=True)
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        "schema_version: 1\n"
        "workspaces:\n"
        "  example_workspace:\n"
        "    placement: portable\n"
        f"    workspace_root: {existing_root.as_posix()}\n",
        encoding="utf-8",
    )
    destination_directory = tmp_path / "consumer" / "workspaces"

    with pytest.raises(ValueError, match="already registered"):
        import_workspace(
            exported_workspace,
            destination_directory=destination_directory,
            workspaces_config_path=config_path,
        )

    assert not destination_directory.exists()


def test_import_workspace_never_overwrites_an_existing_destination(
    tmp_path: Path, exported_workspace: Path
) -> None:
    destination_directory = tmp_path / "consumer" / "workspaces"
    existing_root = destination_directory / "example_workspace"
    _write(existing_root / "keep.txt", b"keep")

    with pytest.raises(FileExistsError, match="already exists"):
        import_workspace(
            exported_workspace,
            destination_directory=destination_directory,
            workspaces_config_path=tmp_path / "consumer" / "workspaces.yaml",
        )

    assert (existing_root / "keep.txt").read_bytes() == b"keep"


def test_import_workspace_rejects_tampered_package_without_creating_destination(
    tmp_path: Path, exported_workspace: Path
) -> None:
    changed_package = tmp_path / "changed.zip"
    with (
        ZipFile(exported_workspace, "r") as source,
        ZipFile(changed_package, "w", compression=ZIP_DEFLATED) as target,
    ):
        for info in source.infolist():
            content = source.read(info)
            if info.filename.endswith("table.csv"):
                content = b"id,value\n1,changed\n"
            target.writestr(info, content)

    destination_directory = tmp_path / "consumer" / "workspaces"
    config_path = tmp_path / "consumer" / "workspaces.yaml"
    with pytest.raises(ValueError, match="mismatch"):
        import_workspace(
            changed_package,
            destination_directory=destination_directory,
            workspaces_config_path=config_path,
        )

    assert not destination_directory.exists()
    assert not config_path.exists()


def test_import_workspace_removes_installed_files_if_registration_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exported_workspace: Path
) -> None:
    destination_directory = tmp_path / "consumer" / "workspaces"
    config_path = tmp_path / "consumer" / "workspaces.yaml"

    def fail_registration(_path: Path, _content: str) -> None:
        raise OSError("registry unavailable")

    monkeypatch.setattr(workspace_registry, "atomic_write_text", fail_registration)

    with pytest.raises(OSError, match="registry unavailable"):
        import_workspace(
            exported_workspace,
            destination_directory=destination_directory,
            workspaces_config_path=config_path,
        )

    assert not (destination_directory / "example_workspace").exists()
    assert not config_path.exists()


def test_import_workspace_preserves_registration_added_during_extraction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, exported_workspace: Path
) -> None:
    config_path = tmp_path / "consumer" / "workspaces.yaml"
    destination_directory = tmp_path / "consumer" / "workspaces"
    original_extract = workspace_import.extract_verified_workspace_export

    def extract_and_register_other_workspace(package_path: str | Path, destination: str | Path):
        manifest = original_extract(package_path, destination)
        other_root = (tmp_path / "consumer" / "other_workspace").resolve()
        (other_root / "data").mkdir(parents=True)
        other_location = WorkspaceLocation.portable(
            workspace_name="other_workspace", workspace_root=other_root
        )
        with workspace_registry.workspace_registration_transaction(
            config_path=config_path, location=other_location
        ):
            pass
        return manifest

    monkeypatch.setattr(
        workspace_import, "extract_verified_workspace_export", extract_and_register_other_workspace
    )

    import_workspace(
        exported_workspace,
        destination_directory=destination_directory,
        workspaces_config_path=config_path,
    )

    registry = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert set(registry["workspaces"]) == {"example_workspace", "other_workspace"}


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
