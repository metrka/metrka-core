"""
Parses and validates YAML configs for the data ingestion pipeline.

The module reads workspace and stream settings, generates target dataset IDs,
and helps find the correct incoming files in the landing zone.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from metrka_core.datasets.dataset_identity import WorkspaceName
from metrka_core.datasets.main_templates import load_main_config
from metrka_core.metadata.artifact import VALID_ARTIFACT_ROLES, ArtifactRole
from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataColumn,
    FilenameMetadataConfig,
    FilenameMetadataValueType,
)
from metrka_core.pipeline.bronze.xlsx_row_assembly import (
    BronzeAssemblyConfig,
    BronzeAssemblyStrategy,
    XlsxReadConfig,
)


@dataclass(frozen=True)
class StreamConfig:
    """Config mapping from source to metrka ingestion."""

    name: str
    official_filename: str
    yaml_contract_name: str | None = None
    artifact_role: ArtifactRole = "data"
    filename_metadata: FilenameMetadataConfig | None = None
    bronze_assembly: BronzeAssemblyConfig | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceConfig:
    workspace_name: WorkspaceName
    streams: dict[str, StreamConfig]
    pipeline: dict[str, Any] = field(default_factory=dict)

    def dataset_id(self, stream_name: str) -> str:
        """Generate a dataset identifier in 'workspace.stream' format."""
        if stream_name not in self.streams:
            raise KeyError(f"Unknown stream: {stream_name}")
        return f"{self.workspace_name}.{stream_name}"

    def find_landed_file(self, stream_name: str, landing_dir: Path) -> Path | None:
        """Locate a single file in the landing zone matching the stream filename."""
        stream = self.streams[stream_name]
        expected = stream.official_filename.upper()

        matches = [
            path
            for path in landing_dir.iterdir()
            if path.is_file() and path.name.upper().endswith(expected)
        ]

        if len(matches) > 1:
            raise RuntimeError(f"Multiple landed files found for stream {stream_name}: {matches}")

        return matches[0] if matches else None

    def find_landed_files_by_pattern(self, stream_name: str, landing_dir: Path) -> tuple[Path, ...]:
        """Locate all landed files matching the stream filename pattern."""
        stream = self.streams[stream_name]
        pattern = stream.official_filename.casefold()

        return tuple(sorted(path for path in landing_dir.glob(pattern) if path.is_file()))


def _load_filename_metadata(
    raw: Any, *, path: Path, stream_name: str
) -> FilenameMetadataConfig | None:
    if raw is None:
        return None

    if not isinstance(raw, dict):
        raise RuntimeError(f"{path}: stream {stream_name} filename_metadata must be a mapping")

    regex = raw.get("regex")

    if not isinstance(regex, str) or not regex.strip():
        raise RuntimeError(f"{path}: stream {stream_name} filename_metadata needs regex")

    raw_columns = raw.get("columns")

    if not isinstance(raw_columns, dict) or not raw_columns:
        raise RuntimeError(f"{path}: stream {stream_name} filename_metadata needs columns")

    columns: dict[str, FilenameMetadataColumn] = {}

    for column_name, column_raw in raw_columns.items():
        if not isinstance(column_name, str) or not column_name.strip():
            raise RuntimeError(
                f"{path}: stream {stream_name} filename metadata "
                "column names must be non-empty strings"
            )

        if not isinstance(column_raw, dict):
            raise RuntimeError(
                f"{path}: stream {stream_name} filename metadata "
                f"column {column_name} must be a mapping"
            )

        from_group = column_raw.get("from_group")
        value_type = column_raw.get("type", "string")
        raw_null_values = column_raw.get("null_values", [])

        if not isinstance(from_group, str) or not from_group.strip():
            raise RuntimeError(
                f"{path}: stream {stream_name} filename metadata "
                f"column {column_name} needs from_group"
            )

        if value_type not in {"string", "integer"}:
            raise RuntimeError(
                f"{path}: stream {stream_name} filename metadata "
                f"column {column_name} has invalid type: {value_type!r}"
            )

        if not isinstance(raw_null_values, list) or not all(
            isinstance(value, str) for value in raw_null_values
        ):
            raise RuntimeError(
                f"{path}: stream {stream_name} filename metadata "
                f"column {column_name} null_values must be a list of strings"
            )

        columns[column_name] = FilenameMetadataColumn(
            from_group=from_group,
            value_type=cast(FilenameMetadataValueType, value_type),
            null_values=tuple(raw_null_values),
        )

    raw_member_key = raw.get("member_key")

    if not isinstance(raw_member_key, list) or not raw_member_key:
        raise RuntimeError(f"{path}: stream {stream_name} filename_metadata needs member_key")

    if not all(
        isinstance(column_name, str) and column_name.strip() for column_name in raw_member_key
    ):
        raise RuntimeError(
            f"{path}: stream {stream_name} filename_metadata member_key must contain strings"
        )

    try:
        return FilenameMetadataConfig(
            regex=regex, columns=columns, member_key=tuple(raw_member_key)
        )
    except ValueError as error:
        raise RuntimeError(
            f"{path}: stream {stream_name} has invalid filename_metadata: {error}"
        ) from error


SKIP_OUTPUT = "skip"


def _load_grouped_outputs(
    raw: Any, *, path: Path, stream_name: str, filename_metadata: FilenameMetadataConfig | None
) -> tuple[str, tuple[tuple[str, str], ...], tuple[str, ...]]:
    """Read ``group_files_by`` and ``outputs``: one Bronze CSV per filename metadata value.

    An output written as ``skip`` leaves the files with that value out of Bronze.
    """
    group_files_by = raw.get("group_files_by")
    outputs = raw.get("outputs")
    where = f"{path}: stream {stream_name} bronze_assembly"

    if not isinstance(group_files_by, str) or not group_files_by.strip():
        raise RuntimeError(f"{where} group_files_by must name a filename metadata column")

    if filename_metadata is None or group_files_by not in filename_metadata.columns:
        known = sorted(filename_metadata.columns) if filename_metadata is not None else []
        raise RuntimeError(
            f"{where} group_files_by {group_files_by!r} is not a filename_metadata column: {known}"
        )

    if "output_filename" in raw:
        raise RuntimeError(f"{where} sets output_filename or group_files_by, not both")

    if not isinstance(outputs, dict) or not outputs:
        raise RuntimeError(
            f"{where} group_files_by needs outputs: a mapping of each {group_files_by} "
            "value to its Bronze CSV, e.g. {crude: <table>.csv}"
        )

    pairs: list[tuple[str, str]] = []
    skipped: list[str] = []

    for value, filename in outputs.items():
        if not isinstance(filename, str):
            raise RuntimeError(f"{where} outputs[{value!r}] must be a CSV filename or 'skip'")

        if filename.strip() == SKIP_OUTPUT:
            skipped.append(str(value))
        else:
            pairs.append((str(value), filename))

    if not pairs:
        raise RuntimeError(f"{where} outputs must name at least one CSV; every value is 'skip'")

    return group_files_by, tuple(pairs), tuple(skipped)


def _load_bronze_assembly(
    raw: Any, *, path: Path, stream_name: str, filename_metadata: FilenameMetadataConfig | None
) -> BronzeAssemblyConfig | None:
    if raw is None:
        return None

    if not isinstance(raw, dict):
        raise RuntimeError(f"{path}: stream {stream_name} bronze_assembly must be a mapping")

    strategy = raw.get("strategy")

    if strategy != "xlsx_rows":
        raise RuntimeError(
            f"{path}: stream {stream_name} has unsupported bronze assembly strategy: {strategy!r}"
        )

    output_filename: str | None = None
    group_files_by: str | None = None
    outputs: tuple[tuple[str, str], ...] = ()
    skip_values: tuple[str, ...] = ()

    if "group_files_by" in raw or "outputs" in raw:
        group_files_by, outputs, skip_values = _load_grouped_outputs(
            raw, path=path, stream_name=stream_name, filename_metadata=filename_metadata
        )
    else:
        output_filename = raw.get("output_filename", f"{stream_name}.csv")

        if not isinstance(output_filename, str):
            raise RuntimeError(
                f"{path}: stream {stream_name} bronze_assembly output_filename must be a string"
            )

    sheet_name = raw.get("sheet_name", 0)
    header_row = raw.get("header_row", 0)
    drop_empty_rows = raw.get("drop_fully_empty_rows", True)
    drop_empty_columns = raw.get("drop_fully_empty_columns", True)

    if isinstance(sheet_name, bool) or not isinstance(sheet_name, (str, int)):
        raise RuntimeError(
            f"{path}: stream {stream_name} bronze_assembly sheet_name must be a string or integer"
        )

    if isinstance(header_row, bool) or not isinstance(header_row, int):
        raise RuntimeError(
            f"{path}: stream {stream_name} bronze_assembly header_row must be an integer"
        )

    if not isinstance(drop_empty_rows, bool):
        raise RuntimeError(
            f"{path}: stream {stream_name} bronze_assembly drop_fully_empty_rows must be boolean"
        )

    if not isinstance(drop_empty_columns, bool):
        raise RuntimeError(
            f"{path}: stream {stream_name} bronze_assembly drop_fully_empty_columns must be boolean"
        )

    try:
        return BronzeAssemblyConfig(
            strategy=cast(BronzeAssemblyStrategy, strategy),
            output_filename=output_filename,
            read_config=XlsxReadConfig(
                sheet_name=sheet_name,
                header_row=header_row,
                drop_fully_empty_rows=drop_empty_rows,
                drop_fully_empty_columns=drop_empty_columns,
            ),
            group_files_by=group_files_by,
            outputs=outputs,
            skip_values=skip_values,
        )
    except ValueError as error:
        raise RuntimeError(
            f"{path}: stream {stream_name} has invalid bronze_assembly: {error}"
        ) from error


def load_source_config(path: str | Path, *, expected_ws_name: str | None = None) -> SourceConfig:
    """Parse and strictly validate a YAML configuration file."""
    path = Path(path)
    raw = load_main_config(path)

    workspace_name = raw.get("workspace_name")
    if not isinstance(workspace_name, str) or not workspace_name.strip():
        raise RuntimeError(f"{path} must define workspace_name")

    if expected_ws_name and workspace_name != expected_ws_name:
        raise RuntimeError(
            f"Unexpected workspace_name in {path}: {workspace_name!r}, "
            f"expected {expected_ws_name!r}"
        )

    raw_streams = raw.get("streams")
    if not isinstance(raw_streams, dict) or not raw_streams:
        raise RuntimeError(f"{path} must define a non-empty streams mapping")

    raw_pipeline = raw.get("pipeline", {})

    if not isinstance(raw_pipeline, dict):
        raise RuntimeError(f"{path}: pipeline must be a mapping")

    streams: dict[str, StreamConfig] = {}

    for stream_name, stream_raw in raw_streams.items():
        if not isinstance(stream_name, str) or not stream_name.strip():
            raise RuntimeError(f"{path}: stream names must be non-empty strings")

        if "." in stream_name:
            raise RuntimeError(f"{path}: stream name must NOT contain '.': {stream_name}")

        if not isinstance(stream_raw, dict):
            raise RuntimeError(f"{path}: stream {stream_name} must be a mapping")

        official_filename = stream_raw.get("official_filename")
        if not isinstance(official_filename, str) or not official_filename.strip():
            raise RuntimeError(f"{path}: stream {stream_name} needs official_filename")

        artifact_role_raw = stream_raw.get("artifact_role", "data")

        if artifact_role_raw not in VALID_ARTIFACT_ROLES:
            raise RuntimeError(
                f"{path}: stream {stream_name} has invalid artifact_role: {artifact_role_raw!r}"
            )

        artifact_role = cast(ArtifactRole, artifact_role_raw)

        filename_metadata = _load_filename_metadata(
            stream_raw.get("filename_metadata"), path=path, stream_name=stream_name
        )

        bronze_assembly = _load_bronze_assembly(
            stream_raw.get("bronze_assembly"),
            path=path,
            stream_name=stream_name,
            filename_metadata=filename_metadata,
        )

        known_keys = {
            "official_filename",
            "yaml_contract_name",
            "artifact_role",
            "filename_metadata",
            "bronze_assembly",
        }
        extra = {key: value for key, value in stream_raw.items() if key not in known_keys}

        yaml_contract_name = stream_raw.get("yaml_contract_name")

        if yaml_contract_name is None and artifact_role == "data":
            yaml_contract_name = f"{stream_name}.yaml"

        streams[stream_name] = StreamConfig(
            name=stream_name,
            official_filename=official_filename,
            yaml_contract_name=yaml_contract_name,
            artifact_role=artifact_role,
            filename_metadata=filename_metadata,
            bronze_assembly=bronze_assembly,
            extra=extra,
        )

    return SourceConfig(
        workspace_name=WorkspaceName(workspace_name), streams=streams, pipeline=dict(raw_pipeline)
    )


def require_dataset_folder_streams(source_config: SourceConfig, *, dataset_name: str) -> None:
    """A dataset folder defines exactly one stream, named like the folder."""

    if list(source_config.streams) != [dataset_name]:
        raise RuntimeError(
            f"Dataset folder {dataset_name!r} must define exactly one stream named "
            f"{dataset_name!r}; found {sorted(source_config.streams)}"
        )
