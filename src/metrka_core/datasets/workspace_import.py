"""Application service for installing a verified customer workspace package."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from metrka_core.datasets.workspace_export import verify_workspace_export
from metrka_core.datasets.workspace_export_archive import extract_verified_workspace_export
from metrka_core.datasets.workspace_location import WorkspaceLocation, WorkspacePlacement
from metrka_core.datasets.workspace_registry import (
    validate_workspace_registration,
    workspace_registration_transaction,
)
from metrka_core.pipeline.composition.workspace_locations import (
    WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE,
    select_workspaces_config_path,
)
from metrka_core.pipeline.config import RuntimeEnvironment, resolve_runtime_environment
from metrka_core.storage.checksums import format_sha256_checksum, sha256_file


@dataclass(frozen=True, slots=True)
class WorkspaceImportResult:
    """Installed portable workspace and its new registry binding."""

    workspace_name: str
    source_placement: WorkspacePlacement
    package_path: Path
    package_checksum: str
    workspace_root: Path
    definition_root: Path
    data_root: Path
    workspaces_config_path: Path
    file_count: int
    total_size_bytes: int


def import_workspace(
    package_path: str | Path,
    *,
    destination_directory: str | Path,
    runtime_environment: RuntimeEnvironment | None = None,
    workspaces_config_path: str | Path | None = None,
) -> WorkspaceImportResult:
    """Verify, install, and register one export as a portable workspace."""

    resolved_package_path = Path(package_path).expanduser().resolve()
    resolved_environment = (
        runtime_environment
        if runtime_environment is not None
        else resolve_runtime_environment(os.environ.get("METRKA_ENV"))
    )
    resolved_config_path = select_workspaces_config_path(
        explicit_config_path=workspaces_config_path,
        environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
        runtime_environment=resolved_environment,
    )
    resolved_destination_directory = Path(destination_directory).expanduser().resolve()
    if resolved_destination_directory.exists() and not resolved_destination_directory.is_dir():
        raise NotADirectoryError(
            f"Workspace import destination is not a directory: {resolved_destination_directory}"
        )

    verification = verify_workspace_export(resolved_package_path)
    workspace_root = resolved_destination_directory / verification.workspace_name
    if workspace_root.exists():
        raise FileExistsError(f"Workspace import destination already exists: {workspace_root}")
    if _is_within(resolved_config_path, workspace_root):
        raise ValueError("Workspace placement configuration must be outside the imported workspace")

    location = WorkspaceLocation.portable(
        workspace_name=verification.workspace_name, workspace_root=workspace_root
    )
    validate_workspace_registration(config_path=resolved_config_path, location=location)

    resolved_destination_directory.mkdir(parents=True, exist_ok=True)
    temporary_directory = Path(
        tempfile.mkdtemp(
            prefix=f".{verification.workspace_name}.import-", dir=resolved_destination_directory
        )
    )
    temporary_workspace = temporary_directory / verification.workspace_name
    installed = False
    try:
        manifest = extract_verified_workspace_export(resolved_package_path, temporary_workspace)
        if manifest.workspace_name != verification.workspace_name:
            raise RuntimeError("Workspace export identity changed during import")
        extracted_package_checksum = format_sha256_checksum(sha256_file(resolved_package_path))
        if extracted_package_checksum != verification.package_checksum:
            raise RuntimeError("Workspace export package changed during import")

        with workspace_registration_transaction(
            config_path=resolved_config_path, location=location
        ):
            if workspace_root.exists():
                raise FileExistsError(
                    f"Workspace import destination already exists: {workspace_root}"
                )
            temporary_workspace.replace(workspace_root)
            installed = True
    except BaseException as error:
        if installed and workspace_root.exists():
            try:
                shutil.rmtree(workspace_root)
            except OSError as cleanup_error:
                error.add_note(
                    f"Workspace import cleanup also failed for {workspace_root}: {cleanup_error}"
                )
        raise
    finally:
        if temporary_directory.exists():
            shutil.rmtree(temporary_directory, ignore_errors=True)

    return WorkspaceImportResult(
        workspace_name=verification.workspace_name,
        source_placement=verification.source_placement,
        package_path=verification.package_path,
        package_checksum=verification.package_checksum,
        workspace_root=workspace_root,
        definition_root=workspace_root,
        data_root=workspace_root / "data",
        workspaces_config_path=resolved_config_path,
        file_count=verification.file_count,
        total_size_bytes=verification.total_size_bytes,
    )


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
