"""Application service for creating a new local workspace."""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

import yaml

from metrka_core.datasets.source_config import load_source_config
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
from metrka_core.pipeline.models import parse_pipeline_spec
from metrka_core.quality.config import parse_quality_config
from metrka_core.storage.atomic_writes import atomic_write_text
from metrka_core.storage.workspace_initializer import LocalWorkspaceInitializer
from metrka_core.storage.workspace_layout import WorkspaceLayout

_IDENTIFIER_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")


@dataclass(frozen=True, slots=True)
class WorkspaceInitializationResult:
    """Paths created and registered for one new workspace."""

    workspace_name: str
    placement: WorkspacePlacement
    workspace_root: Path | None
    definition_root: Path
    data_root: Path
    workspaces_config_path: Path
    main_config_path: Path
    quality_config_path: Path


def initialize_workspace(
    workspace_name: str,
    *,
    download_url: str,
    runtime_environment: RuntimeEnvironment | None = None,
    workspaces_config_path: str | Path | None = None,
    placement: WorkspacePlacement | str = WorkspacePlacement.PORTABLE,
    workspace_root: str | Path | None = None,
    definition_root: str | Path | None = None,
    data_root: str | Path | None = None,
    stream_name: str = "data",
    official_filename: str | None = None,
    source_name: str | None = None,
) -> WorkspaceInitializationResult:
    """Create and register a Bronze-ready HTTP workspace."""

    normalized_workspace_name = _identifier(workspace_name, field_name="workspace_name")
    normalized_stream_name = _identifier(stream_name, field_name="stream_name")
    normalized_download_url = _download_url(download_url)
    normalized_official_filename = _official_filename(
        official_filename=official_filename, download_url=normalized_download_url
    )
    normalized_source_name = _source_name(
        source_name=source_name, workspace_name=normalized_workspace_name
    )

    resolved_environment = (
        runtime_environment
        if runtime_environment is not None
        else resolve_runtime_environment(os.environ.get("METRKA_ENV"))
    )
    resolved_workspaces_config_path = select_workspaces_config_path(
        explicit_config_path=workspaces_config_path,
        environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
        runtime_environment=resolved_environment,
    )
    normalized_placement = _placement(placement)
    location = _requested_location(
        workspace_name=normalized_workspace_name,
        placement=normalized_placement,
        config_path=resolved_workspaces_config_path,
        workspace_root=workspace_root,
        definition_root=definition_root,
        data_root=data_root,
    )
    validate_workspace_registration(config_path=resolved_workspaces_config_path, location=location)
    created_roots = _created_roots(location)

    for root in created_roots:
        if root.exists():
            raise FileExistsError(f"Workspace root already exists: {root}")

    source_config = _source_config(
        workspace_name=normalized_workspace_name,
        stream_name=normalized_stream_name,
        official_filename=normalized_official_filename,
        download_url=normalized_download_url,
    )
    quality_config = _quality_config()
    _validate_generated_configuration(source_config=source_config, quality_config=quality_config)

    owned_roots: tuple[Path, ...] = ()
    try:
        with workspace_registration_transaction(
            config_path=resolved_workspaces_config_path, location=location
        ):
            for root in created_roots:
                if root.exists():
                    raise FileExistsError(f"Workspace root already exists: {root}")
            owned_roots = created_roots

            layout = WorkspaceLayout(location=location)
            LocalWorkspaceInitializer(layout=layout).ensure_structure()
            layout.conf_dir.mkdir(parents=True, exist_ok=True)

            main_config_path = layout.config_path("main.yaml")
            quality_config_path = layout.config_path("quality.yaml")

            atomic_write_text(main_config_path, _yaml_text(source_config))
            atomic_write_text(quality_config_path, _yaml_text(quality_config))
            atomic_write_text(
                layout.definition_root / "README.md",
                _workspace_readme(normalized_workspace_name, normalized_source_name),
            )

            if location.is_portable:
                atomic_write_text(layout.definition_root / ".gitignore", "data/\n")

            load_source_config(main_config_path, expected_ws_name=normalized_workspace_name)
    except BaseException as error:
        _cleanup_created_roots(created_roots=owned_roots, original_error=error)

        raise

    return WorkspaceInitializationResult(
        workspace_name=normalized_workspace_name,
        placement=location.placement,
        workspace_root=location.workspace_root,
        definition_root=location.definition_root,
        data_root=location.data_root,
        workspaces_config_path=resolved_workspaces_config_path,
        main_config_path=location.definition_root / "conf" / "main.yaml",
        quality_config_path=location.definition_root / "conf" / "quality.yaml",
    )


def _identifier(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"{field_name} must start with a lowercase letter and contain only "
            "lowercase letters, digits, and underscores"
        )

    return value


def _download_url(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("download_url must be a non-empty HTTP or HTTPS URL")

    normalized = value.strip()
    parsed = urlsplit(normalized)

    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("download_url must be an absolute HTTP or HTTPS URL")

    return normalized


def _official_filename(*, official_filename: str | None, download_url: str) -> str:
    resolved = official_filename

    if resolved is None:
        resolved = PurePosixPath(unquote(urlsplit(download_url).path)).name

    if not isinstance(resolved, str) or not resolved.strip():
        raise ValueError(
            "official_filename is required when download_url has no filename component"
        )

    normalized = resolved.strip()

    if normalized in {".", ".."} or "/" in normalized or "\\" in normalized:
        raise ValueError("official_filename must be a filename, not a path")

    return normalized


def _source_name(*, source_name: str | None, workspace_name: str) -> str:
    if source_name is None:
        return workspace_name.replace("_", " ").title()

    if not isinstance(source_name, str) or not source_name.strip():
        raise ValueError("source_name must be a non-empty string")

    return source_name.strip()


def _resolve_path(value: str | Path, *, relative_to: Path) -> Path:
    if isinstance(value, str) and not value.strip():
        raise ValueError("Workspace root paths must not be blank")

    candidate = Path(value).expanduser()

    if not candidate.is_absolute():
        candidate = relative_to / candidate

    return candidate.resolve()


def _placement(value: WorkspacePlacement | str) -> WorkspacePlacement:
    try:
        return WorkspacePlacement(value)
    except ValueError as error:
        supported = [item.value for item in WorkspacePlacement]
        raise ValueError(f"placement must be one of {supported}") from error


def _requested_location(
    *,
    workspace_name: str,
    placement: WorkspacePlacement,
    config_path: Path,
    workspace_root: str | Path | None,
    definition_root: str | Path | None,
    data_root: str | Path | None,
) -> WorkspaceLocation:
    relative_to = config_path.parent

    if placement is WorkspacePlacement.PORTABLE:
        if workspace_root is None:
            raise ValueError("workspace_root is required for portable placement")

        if definition_root is not None or data_root is not None:
            raise ValueError(
                "Portable placement accepts workspace_root, not definition_root or data_root"
            )

        return WorkspaceLocation.portable(
            workspace_name=workspace_name,
            workspace_root=_resolve_path(workspace_root, relative_to=relative_to),
        )

    if workspace_root is not None:
        raise ValueError(
            "Managed placement accepts definition_root and data_root, not workspace_root"
        )

    if definition_root is None or data_root is None:
        raise ValueError("definition_root and data_root are required for managed placement")

    return WorkspaceLocation.managed(
        workspace_name=workspace_name,
        definition_root=_resolve_path(definition_root, relative_to=relative_to),
        data_root=_resolve_path(data_root, relative_to=relative_to),
    )


def _created_roots(location: WorkspaceLocation) -> tuple[Path, ...]:
    if location.workspace_root is not None:
        return (location.workspace_root,)

    return (location.definition_root, location.data_root)


def _cleanup_created_roots(
    *, created_roots: tuple[Path, ...], original_error: BaseException
) -> None:
    for root in sorted(created_roots, key=lambda path: len(path.parts), reverse=True):
        if not root.exists():
            continue

        try:
            shutil.rmtree(root)
        except OSError as cleanup_error:
            original_error.add_note(f"Workspace cleanup also failed for {root}: {cleanup_error}")


def _source_config(
    *, workspace_name: str, stream_name: str, official_filename: str, download_url: str
) -> dict[str, object]:
    # Processing settings only. The dataset's title, description and source are
    # described in its contract's `meta` block.
    return {
        "workspace_name": workspace_name,
        "streams": {
            stream_name: {"official_filename": official_filename, "download_url": download_url}
        },
        "pipeline": {
            "quality": {"config": "quality.yaml"},
            "acquisition": {
                "extractor": "http.files",
                "options": {
                    "timeout_seconds": 120,
                    "min_bytes": 1,
                    "user_agent": "Metrka data pipeline",
                },
                "backfill": {"match_mode": "exact"},
            },
            "steps": [{"action": "bronze.ingest"}],
        },
    }


def _quality_config() -> dict[str, object]:
    # Built-in checks run automatically. Add data rules under "tables" once the
    # workspace has a Silver contract.
    return {"version": 1}


def _validate_generated_configuration(
    *, source_config: dict[str, object], quality_config: dict[str, object]
) -> None:
    pipeline = source_config["pipeline"]

    if not isinstance(pipeline, dict):
        raise TypeError("Generated pipeline configuration must be a mapping")

    parse_pipeline_spec(pipeline)
    parse_quality_config(quality_config, source="<workspace scaffold>")


def _yaml_text(value: object) -> str:
    return yaml.safe_dump(value, sort_keys=False, allow_unicode=True)


def _workspace_readme(workspace_name: str, source_name: str) -> str:
    return (
        f"# {workspace_name}\n\n"
        f"Source: {source_name}.\n\n"
        "This workspace is configured for HTTP acquisition and Bronze preservation.\n\n"
        "Before adding `silver.process`, create a dataset contract and describe the dataset "
        "in its `meta` block: `title`, `info`, and `source` (`publisher`, `name`, `url`, "
        "`update_frequency`). Declare the Silver input and output settings in "
        "`conf/main.yaml`, and describe every published column in the contract.\n"
    )
