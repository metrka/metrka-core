"""Verify that an extracted customer export works as a portable workspace."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

import pyarrow.parquet as parquet

_CHECKSUM_PATTERN = re.compile(r"sha256:([0-9a-f]{64})\Z")
_TEXT_SUFFIXES = frozenset(
    {".csv", ".json", ".jsonl", ".md", ".py", ".sql", ".txt", ".yaml", ".yml"}
)


class RoundTripVerificationError(RuntimeError):
    """Raised when an extracted customer workspace is not self-contained."""


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("workspace_root", type=Path)
    parser.add_argument("--workspace-name", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--forbidden-source-root", type=Path, action="append", required=True)
    return parser.parse_args()


def _mapping(value: Any, description: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise RoundTripVerificationError(f"{description} must be a JSON object")
    return value


def _sequence(value: Any, description: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, list):
        raise RoundTripVerificationError(f"{description} must be a JSON array")
    return value


def _string(value: Any, description: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RoundTripVerificationError(f"{description} must be a non-empty string")
    return value.strip()


def _integer(value: Any, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RoundTripVerificationError(f"{description} must be a non-negative integer")
    return value


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RoundTripVerificationError(f"Could not read JSON file {path}: {error}") from error
    return _mapping(value, str(path))


def _resolve_relative(root: Path, value: Any, description: str) -> Path:
    relative_value = _string(value, description)
    if "\\" in relative_value:
        raise RoundTripVerificationError(f"{description} must use POSIX separators")

    relative_path = PurePosixPath(relative_value)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise RoundTripVerificationError(f"{description} must remain relative: {relative_value}")

    resolved = root.joinpath(*relative_path.parts).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise RoundTripVerificationError(
            f"{description} escapes its owning root: {relative_value}"
        ) from error
    return resolved


def _sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _expected_digest(value: Any, description: str) -> str:
    checksum = _string(value, description)
    match = _CHECKSUM_PATTERN.fullmatch(checksum)
    if match is None:
        raise RoundTripVerificationError(f"{description} is not a canonical SHA-256 checksum")
    return match.group(1)


def _verify_file(
    *, path: Path, checksum: Any, description: str, expected_size: Any | None = None
) -> None:
    if not path.is_file():
        raise RoundTripVerificationError(f"{description} is missing: {path}")

    if expected_size is not None:
        size = _integer(expected_size, f"{description} size")
        if path.stat().st_size != size:
            raise RoundTripVerificationError(
                f"{description} size mismatch: expected {size}, found {path.stat().st_size}"
            )

    expected_digest = _expected_digest(checksum, f"{description} checksum")
    actual_digest = _sha256_file(path)
    if actual_digest != expected_digest:
        raise RoundTripVerificationError(
            f"{description} checksum mismatch: expected {expected_digest}, found {actual_digest}"
        )


def _assert_no_absolute_path_values(value: Any, *, key: str = "root") -> None:
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            _assert_no_absolute_path_values(child_value, key=str(child_key))
        return

    if isinstance(value, list):
        for child_value in value:
            _assert_no_absolute_path_values(child_value, key=key)
        return

    if not isinstance(value, str) or "path" not in key.lower():
        return

    if PurePosixPath(value).is_absolute() or PureWindowsPath(value).is_absolute():
        raise RoundTripVerificationError(f"Extracted JSON contains an absolute {key}: {value}")


def _assert_source_root_absent(workspace_root: Path, source_root: Path) -> None:
    forbidden_values = {str(source_root.resolve()), source_root.resolve().as_posix()}
    for path in workspace_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in _TEXT_SUFFIXES:
            continue
        content = path.read_text(encoding="utf-8", errors="replace")
        if any(forbidden in content for forbidden in forbidden_values):
            raise RoundTripVerificationError(f"Extracted file leaks the source root: {path}")


def _verify_csv(path: Path, table: Mapping[str, Any]) -> None:
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        try:
            header = next(reader)
        except StopIteration as error:
            raise RoundTripVerificationError(f"Silver CSV is empty: {path}") from error
        row_count = sum(1 for _ in reader)

    expected_rows = _integer(table.get("row_count"), f"{path} row_count")
    expected_columns = _integer(table.get("column_count"), f"{path} column_count")
    if row_count != expected_rows or len(header) != expected_columns:
        raise RoundTripVerificationError(
            f"Silver CSV shape mismatch for {path}: "
            f"expected ({expected_rows}, {expected_columns}), "
            f"found ({row_count}, {len(header)})"
        )


def _verify_parquet(path: Path, table: Mapping[str, Any]) -> None:
    metadata = parquet.ParquetFile(path).metadata
    expected_rows = _integer(table.get("row_count"), f"{path} row_count")
    expected_columns = _integer(table.get("column_count"), f"{path} column_count")
    if metadata.num_rows != expected_rows or metadata.num_columns != expected_columns:
        raise RoundTripVerificationError(
            f"Silver Parquet shape mismatch for {path}: "
            f"expected ({expected_rows}, {expected_columns}), "
            f"found ({metadata.num_rows}, {metadata.num_columns})"
        )


def verify_roundtrip(
    *, workspace_root: Path, workspace_name: str, dataset_id: str, source_roots: Sequence[Path]
) -> None:
    root = workspace_root.expanduser().resolve()
    data_root = root / "data"
    resolved_source_roots = {source_root.expanduser().resolve() for source_root in source_roots}
    if root in resolved_source_roots:
        raise RoundTripVerificationError("Round-trip workspace must not reuse the source root")

    export_manifest = _load_json(root / "metrka-workspace-manifest.json")
    if export_manifest.get("workspace_name") != workspace_name:
        raise RoundTripVerificationError("Export manifest workspace_name mismatch")

    for source_root in resolved_source_roots:
        _assert_source_root_absent(root, source_root)
    for json_path in root.rglob("*.json"):
        _assert_no_absolute_path_values(_load_json(json_path))

    silver_manifests = sorted((data_root / "files" / "silver" / "manifests").rglob("*.json"))
    if not silver_manifests:
        raise RoundTripVerificationError("Extracted workspace contains no Silver manifests")

    verified_formats: set[str] = set()
    for manifest_path in silver_manifests:
        manifest = _load_json(manifest_path)
        if manifest.get("dataset_id") != dataset_id:
            raise RoundTripVerificationError(
                f"Silver manifest dataset_id mismatch: {manifest_path}"
            )

        contract = _mapping(manifest.get("contract"), f"{manifest_path} contract")
        definition_path = _resolve_relative(root, contract.get("path"), "contract path")
        if not definition_path.is_file():
            raise RoundTripVerificationError(f"Contract definition is missing: {definition_path}")

        snapshot_path = _resolve_relative(
            data_root, contract.get("snapshot_path"), "contract snapshot path"
        )
        _verify_file(
            path=snapshot_path, checksum=contract.get("checksum"), description="contract snapshot"
        )

        tables = _sequence(manifest.get("tables"), f"{manifest_path} tables")
        for index, raw_table in enumerate(tables):
            table = _mapping(raw_table, f"{manifest_path} table {index}")
            table_path = _resolve_relative(
                data_root, table.get("path"), f"{manifest_path} table {index} path"
            )
            _verify_file(
                path=table_path,
                checksum=table.get("checksum"),
                expected_size=table.get("size_bytes"),
                description=f"Silver table {index}",
            )

            table_format = _string(table.get("format"), f"{table_path} format")
            verified_formats.add(table_format)
            if table_format == "csv":
                _verify_csv(table_path, table)
            elif table_format == "parquet":
                _verify_parquet(table_path, table)
            else:
                raise RoundTripVerificationError(
                    f"Unexpected Silver table format in round-trip export: {table_format}"
                )

    if verified_formats != {"csv", "parquet"}:
        raise RoundTripVerificationError(
            f"Expected exported CSV and Parquet tables, found {sorted(verified_formats)}"
        )

    pointer_paths = sorted((data_root / "current" / "latest" / "silver").glob("*.json"))
    if len(pointer_paths) != 1:
        raise RoundTripVerificationError(
            f"Expected one current Silver pointer, found {len(pointer_paths)}"
        )

    pointer = _load_json(pointer_paths[0])
    if pointer.get("dataset_id") != dataset_id:
        raise RoundTripVerificationError("Current Silver pointer dataset_id mismatch")

    referenced_manifest = _resolve_relative(
        data_root, pointer.get("manifest_path"), "current pointer manifest_path"
    )
    if referenced_manifest not in {path.resolve() for path in silver_manifests}:
        raise RoundTripVerificationError(
            f"Current pointer references an unknown Silver manifest: {referenced_manifest}"
        )

    view_paths = _sequence(pointer.get("view_paths"), "current pointer view_paths")
    if not view_paths:
        raise RoundTripVerificationError("Current Silver pointer has no view paths")
    for index, value in enumerate(view_paths):
        view_path = _resolve_relative(data_root, value, f"current pointer view_paths[{index}]")
        if not view_path.is_file():
            raise RoundTripVerificationError(f"Current Silver view is missing: {view_path}")

    print(f"Round-trip workspace verified: {root}")
    print(f"Silver manifests: {len(silver_manifests)}")
    print(f"Silver formats: {', '.join(sorted(verified_formats))}")
    print(f"Current publication: {_string(pointer.get('publication_id'), 'publication_id')}")


def main() -> int:
    args = _arguments()
    verify_roundtrip(
        workspace_root=args.workspace_root,
        workspace_name=args.workspace_name,
        dataset_id=args.dataset_id,
        source_roots=args.forbidden_source_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
