"""Application service for assembling portable customer workspace packages."""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Final

from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.workspace_export_archive import (
    WorkspaceExportSourceFile,
    verify_workspace_export,
    write_workspace_export,
)
from metrka_core.datasets.workspace_export_content_policy import (
    WorkspaceExportContentPolicyError,
    WorkspaceExportPolicyViolation,
    validate_workspace_export_definition_files,
)
from metrka_core.datasets.workspace_export_models import (
    WORKSPACE_EXPORT_MANIFEST_NAME,
    WorkspaceExportFile,
    WorkspaceExportFileRole,
    WorkspaceExportIntegrityError,
    WorkspaceExportManifest,
    WorkspaceExportResult,
    WorkspaceExportVerificationResult,
    validate_utc_timestamp,
    validate_workspace_name,
)
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.pipeline.composition.workspace_locations import (
    WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE,
    build_workspace_location_resolver,
)
from metrka_core.pipeline.config import RuntimeEnvironment, resolve_runtime_environment
from metrka_core.pipeline.runtime_services import Clock, SystemClock
from metrka_core.storage.checksums import format_sha256_checksum, sha256_file
from metrka_core.storage.workspace_layout import WorkspaceLayout

_NON_PRODUCT_DIRECTORY_NAMES: Final = frozenset(
    {
        ".git",
        ".hg",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".svn",
        ".venv",
        "__pycache__",
        "venv",
    }
)
_NON_PRODUCT_FILE_SUFFIXES: Final = frozenset({".pyc", ".pyo"})

__all__ = [
    "WorkspaceExportContentPolicyError",
    "WorkspaceExportPolicyViolation",
    "WorkspaceExportIntegrityError",
    "WorkspaceExportResult",
    "WorkspaceExportVerificationResult",
    "export_workspace",
    "verify_workspace_export",
]


def export_workspace(
    workspace_name: str,
    destination: str | Path,
    *,
    runtime_environment: RuntimeEnvironment | None = None,
    workspaces_config_path: str | Path | None = None,
    workspace_location_resolver: WorkspaceLocationResolver | None = None,
    clock: Clock | None = None,
    overwrite: bool = False,
) -> WorkspaceExportResult:
    """Resolve one workspace and assemble it as a verified portable ZIP package."""

    if not isinstance(workspace_name, str) or not workspace_name.strip():
        raise ValueError("workspace_name must be a non-empty string")
    if workspace_location_resolver is not None and workspaces_config_path is not None:
        raise ValueError(
            "Pass either workspace_location_resolver or workspaces_config_path, not both"
        )

    resolved_environment = (
        runtime_environment
        if runtime_environment is not None
        else resolve_runtime_environment(os.environ.get("METRKA_ENV"))
    )
    resolved_locations = (
        workspace_location_resolver
        if workspace_location_resolver is not None
        else build_workspace_location_resolver(
            explicit_config_path=workspaces_config_path,
            environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
            runtime_environment=resolved_environment,
        )
    )
    normalized_workspace_name = workspace_name.strip()
    location = resolved_locations.resolve(normalized_workspace_name)
    if location.workspace_name != normalized_workspace_name:
        raise ValueError(
            "WorkspaceLocationResolver returned a location for a different workspace: "
            f"{location.workspace_name!r}"
        )

    return _export_workspace_location(
        location=location,
        destination=Path(destination),
        clock=clock if clock is not None else SystemClock(),
        overwrite=overwrite,
    )


def _export_workspace_location(
    *, location: WorkspaceLocation, destination: Path, clock: Clock, overwrite: bool
) -> WorkspaceExportResult:
    validate_workspace_name(location.workspace_name)
    definition_root = location.definition_root.resolve()
    data_root = location.data_root.resolve()
    if not definition_root.is_dir():
        raise RuntimeError(f"Workspace definition_root is not a directory: {definition_root}")
    if not data_root.is_dir():
        raise RuntimeError(f"Workspace data_root is not a directory: {data_root}")

    layout = WorkspaceLayout(location=location)
    _require_quiescent_workspace(layout)
    resolved_destination = destination.expanduser().resolve()
    for source_root in (definition_root, data_root):
        if _is_within(resolved_destination, source_root):
            raise ValueError("Workspace export destination must be outside the source workspace")

    source_files = _collect_source_files(location)
    created_at = clock.now_utc()
    validate_utc_timestamp(created_at, field_name="clock.now_utc()")
    manifest = WorkspaceExportManifest(
        workspace_name=location.workspace_name,
        source_placement=location.placement,
        created_at=created_at,
        files=tuple(source.manifest_entry for source in source_files),
    )
    verification = write_workspace_export(
        destination=resolved_destination,
        manifest=manifest,
        source_files=source_files,
        overwrite=overwrite,
    )
    return WorkspaceExportResult(
        workspace_name=manifest.workspace_name,
        source_placement=manifest.source_placement,
        package_path=verification.package_path,
        package_checksum=verification.package_checksum,
        file_count=manifest.file_count,
        total_size_bytes=manifest.total_size_bytes,
    )


def _collect_source_files(location: WorkspaceLocation) -> tuple[WorkspaceExportSourceFile, ...]:
    definitions = _collect_root_files(
        root=location.definition_root,
        role=WorkspaceExportFileRole.DEFINITION,
        archive_prefix=None,
        excluded_roots=(location.data_root,) if location.is_portable else (),
    )
    validate_workspace_export_definition_files(
        (source.manifest_entry.path, source.source_path) for source in definitions
    )
    layout = WorkspaceLayout(location=location)
    data = _collect_root_files(
        root=location.data_root,
        role=WorkspaceExportFileRole.DATA,
        archive_prefix=PurePosixPath("data"),
        excluded_roots=(layout.silver_dir / "staging",),
    )
    combined = sorted((*definitions, *data), key=lambda item: item.manifest_entry.path)
    paths = [item.manifest_entry.path for item in combined]
    if len(paths) != len(set(paths)) or len(paths) != len({path.casefold() for path in paths}):
        raise ValueError(
            "Workspace definitions collide with the reserved portable data/ package path"
        )
    if WORKSPACE_EXPORT_MANIFEST_NAME.casefold() in {path.casefold() for path in paths}:
        raise ValueError(
            f"Workspace definition uses reserved export path {WORKSPACE_EXPORT_MANIFEST_NAME!r}"
        )
    return tuple(combined)


def _collect_root_files(
    *,
    root: Path,
    role: WorkspaceExportFileRole,
    archive_prefix: PurePosixPath | None,
    excluded_roots: tuple[Path, ...],
) -> tuple[WorkspaceExportSourceFile, ...]:
    resolved_root = root.resolve()
    resolved_excluded = {excluded_root.resolve() for excluded_root in excluded_roots}
    collected: list[WorkspaceExportSourceFile] = []

    def raise_walk_error(error: OSError) -> None:
        raise error

    for current_raw, directory_names, file_names in os.walk(
        resolved_root, topdown=True, onerror=raise_walk_error, followlinks=False
    ):
        current = Path(current_raw)
        retained_directories: list[str] = []
        for directory_name in directory_names:
            directory = current / directory_name
            if directory_name.casefold() in _NON_PRODUCT_DIRECTORY_NAMES:
                continue
            if directory.resolve() in resolved_excluded:
                continue
            if _is_link_like(directory):
                raise ValueError(f"Workspace export does not follow linked directory: {directory}")
            retained_directories.append(directory_name)
        directory_names[:] = retained_directories

        for file_name in file_names:
            source_path = current / file_name
            if source_path.suffix.casefold() in _NON_PRODUCT_FILE_SUFFIXES:
                continue
            if _is_link_like(source_path):
                raise ValueError(f"Workspace export does not follow linked file: {source_path}")
            resolved_source = source_path.resolve(strict=True)
            try:
                relative = resolved_source.relative_to(resolved_root)
            except ValueError as error:
                raise ValueError(
                    f"Workspace export file escapes its configured root: {source_path}"
                ) from error
            if not resolved_source.is_file():
                raise ValueError(f"Workspace export source is not a regular file: {source_path}")

            archive_path = PurePosixPath(relative.as_posix())
            if archive_prefix is not None:
                archive_path = archive_prefix / archive_path
            entry = WorkspaceExportFile(
                path=archive_path.as_posix(),
                role=role,
                size_bytes=resolved_source.stat().st_size,
                checksum=format_sha256_checksum(sha256_file(resolved_source)),
            )
            collected.append(
                WorkspaceExportSourceFile(source_path=resolved_source, manifest_entry=entry)
            )

    return tuple(collected)


def _require_quiescent_workspace(layout: WorkspaceLayout) -> None:
    markers = (layout.bronze_execution_marker_path, layout.silver_execution_marker_path)
    active = [marker for marker in markers if marker.exists()]
    if active:
        raise RuntimeError(
            f"Workspace export requires a quiescent workspace; active execution markers: {active}"
        )


def _is_link_like(path: Path) -> bool:
    return path.is_symlink() or (os.name == "nt" and os.path.isjunction(path))


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
