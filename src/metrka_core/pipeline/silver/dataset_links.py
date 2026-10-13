"""Names that main.yaml, the Silver contract and quality.yaml share.

A stream reaches its contract tables through these links: the Bronze file it writes feeds the
table named like the file, and Silver settings such as ``version_period``, ``temporal_coverage``
and catalog highlights name contract tables and columns. A contract with one table needs none
of these names repeated: its only Bronze file feeds it, and settings that omit ``table`` use it.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from pathlib import Path
from typing import Any

from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.quality.config import contract_tables
from metrka_core.storage.config_store import ConfigStore


def match_bronze_tables(
    bronze_dir: Path, table_keys: Collection[str]
) -> tuple[tuple[str, Path], ...]:
    """Pair each Bronze file with the contract table it feeds, in file-name order.

    A file feeds the table named like the file without its extension. When the contract has
    one table and Bronze holds one file, that file feeds the table whatever its name.
    """

    files = sorted(path for path in bronze_dir.iterdir() if path.is_file())
    matched = tuple((path.stem, path) for path in files if path.stem in table_keys)

    if matched or len(table_keys) != 1 or len(files) != 1:
        return matched

    (table_key,) = tuple(table_keys)
    return ((table_key, files[0]),)


def unmatched_bronze_message(
    *, dataset_id: str, bronze_dir: Path, table_keys: Collection[str]
) -> str:
    """Explain which names failed to match when no Bronze file feeds a contract table."""

    files = sorted(path.name for path in bronze_dir.iterdir() if path.is_file())

    return (
        f"No Bronze files matched the configured Silver tables for dataset_id={dataset_id}: "
        f"Bronze files {files}, contract tables {sorted(table_keys)}. A Bronze file feeds the "
        "table named like the file without its extension; rename the table or set "
        "bronze_assembly.output_filename to '<table>.csv'."
    )


def validate_dataset_links(
    *,
    source_config: SourceConfig,
    config_store: ConfigStore,
    contracts: Mapping[Path, Mapping[str, Any]],
) -> None:
    """Check every name a stream's configuration shares with its Silver contract.

    All mismatches are reported together, each with both names.
    """

    problems: list[str] = []

    for stream_name, stream in source_config.streams.items():
        if stream.artifact_role != "data" or stream.yaml_contract_name is None:
            continue

        contract = contracts.get(config_store.path(name=stream.yaml_contract_name))

        if contract is None:
            continue

        tables: Mapping[str, Any] = contract.get("tables", {})
        published = contract_tables(contract)
        silver = stream.extra.get("silver", {})
        where = f"stream {stream_name!r}"

        problems += _bronze_file_problems(where=where, stream=stream, table_keys=tables)
        problems += _version_period_problems(
            where=where, silver=silver, bronze_columns=_bronze_columns(stream, tables)
        )
        problems += _table_reference_problems(
            where=f"{where} silver.temporal_coverage",
            reference=silver.get("temporal_coverage"),
            published=published,
        )

        for position, highlight in enumerate(stream.extra.get("catalog", {}).get("highlights", [])):
            problems += _table_reference_problems(
                where=f"{where} catalog.highlights[{position}]",
                reference=highlight,
                published=published,
            )

    if problems:
        raise RuntimeError("Dataset configuration names do not match:\n- " + "\n- ".join(problems))


def _bronze_file_problems(
    *, where: str, stream: StreamConfig, table_keys: Collection[str]
) -> list[str]:
    # Only an assembled stream names its Bronze file before the run.
    if stream.bronze_assembly is None or len(table_keys) == 1:
        return []

    file_name = stream.bronze_assembly.output_filename

    if Path(file_name).stem in table_keys:
        return []

    return [
        f"{where}: Bronze file {file_name!r} (bronze_assembly.output_filename) feeds no contract "
        f"table; contract tables: {sorted(table_keys)}. Name the table "
        f"{Path(file_name).stem!r} or set output_filename to '<table>.csv'."
    ]


def _bronze_columns(stream: StreamConfig, tables: Mapping[str, Any]) -> set[str]:
    """Column names in Bronze files: contract source columns and filename metadata."""

    columns = {
        str(source_column)
        for table in tables.values()
        for source_column in table.get("columns", {})
    }

    if stream.filename_metadata is not None:
        columns |= set(stream.filename_metadata.columns)

    return columns


def _version_period_problems(
    *, where: str, silver: Mapping[str, Any], bronze_columns: set[str]
) -> list[str]:
    version_period = silver.get("version_period")
    column = version_period.get("column") if isinstance(version_period, Mapping) else None

    if column is None or column in bronze_columns:
        return []

    return [
        f"{where} silver.version_period.column {column!r} is not a Bronze column. It is read "
        "from Bronze before renaming, so use a contract source column or filename metadata "
        f"column: {sorted(bronze_columns)}."
    ]


def _table_reference_problems(
    *, where: str, reference: object, published: Mapping[str, Mapping[str, str]]
) -> list[str]:
    """Check a ``table``/``column`` pair that reads a finished Silver table."""

    if not isinstance(reference, Mapping):
        return []

    table_key = reference.get("table")

    if table_key is None:
        if len(published) != 1:
            return [
                f"{where} needs 'table' because the contract has several tables: "
                f"{sorted(published)}."
            ]

        (table_key,) = tuple(published)

    if table_key not in published:
        return [f"{where}.table {table_key!r} is not a contract table: {sorted(published)}."]

    column = reference.get("column")

    if column is None or column in published[table_key]:
        return []

    return [
        f"{where}.column {column!r} is not a published column of table {table_key!r}. It is "
        f"read after renaming, so use a published name: {sorted(published[table_key])}."
    ]
