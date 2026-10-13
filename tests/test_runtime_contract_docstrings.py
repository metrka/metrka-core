"""Protect documentation on registry- and extension-facing contracts."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from metrka_core.metadata import source_schema
from metrka_core.metadata.source_schema import (
    ParsedSourceSchema,
    SourceSchemaChange,
    compare_source_schema_fields,
)
from metrka_core.pipeline.actions import bronze, documentation, silver
from metrka_core.pipeline.silver import (
    manage_engine_releases,
    manage_publication_candidates,
    reconcile_publications,
)
from metrka_core.quality import checks as quality_checks
from metrka_core.quality import config as quality_config
from metrka_core.quality import gates as quality_gates
from metrka_core.quality.checks import files, outputs, tables

CONTRACT_OBJECTS: tuple[tuple[str, Any], ...] = (
    ("parse_bronze_ingest_options", bronze.parse_bronze_ingest_options),
    ("ingest_bronze_action", bronze.ingest_bronze_action),
    ("parse_documentation_bind_options", documentation.parse_documentation_bind_options),
    ("bind_documentation_action", documentation.bind_documentation_action),
    ("parse_silver_process_options", silver.parse_silver_process_options),
    ("process_silver_action", silver.process_silver_action),
    ("source_schema module", source_schema),
    ("ParsedSourceSchema.table_count", ParsedSourceSchema.table_count),
    ("ParsedSourceSchema.field_count", ParsedSourceSchema.field_count),
    ("ParsedSourceSchema.schema_hash_algorithm", ParsedSourceSchema.schema_hash_algorithm),
    ("ParsedSourceSchema.schema_hash", ParsedSourceSchema.schema_hash),
    ("SourceSchemaChange", SourceSchemaChange),
    ("compare_source_schema_fields", compare_source_schema_fields),
    ("manage_engine_releases.main", manage_engine_releases.main),
    ("manage_publication_candidates.main", manage_publication_candidates.main),
    ("reconcile_publications.main", reconcile_publications.main),
)

QUALITY_MODULES: tuple[tuple[str, Any], ...] = (
    ("quality.checks", quality_checks),
    ("quality.checks.files", files),
    ("quality.checks.outputs", outputs),
    ("quality.checks.tables", tables),
    ("quality.config", quality_config),
    ("quality.gates", quality_gates),
)


@pytest.mark.parametrize(
    ("name", "contract"), CONTRACT_OBJECTS, ids=[x[0] for x in CONTRACT_OBJECTS]
)
def test_registry_and_extension_contracts_have_docstrings(name: str, contract: Any) -> None:
    """Require documentation where callers cannot infer a contract from a call site."""

    assert inspect.getdoc(contract), f"Contract has no docstring: {name}"


@pytest.mark.parametrize(("name", "module"), QUALITY_MODULES, ids=[x[0] for x in QUALITY_MODULES])
def test_builtin_quality_modules_describe_their_contract(name: str, module: Any) -> None:
    """Keep gate-context documentation adjacent to built-in checks."""

    assert inspect.getdoc(module), f"Quality module has no docstring: {name}"


def test_builtin_quality_checks_have_docstrings() -> None:
    """The first docstring line becomes the check description stored with evidence."""

    undocumented = [
        f"{module.__name__}.{name}"
        for module in (files, outputs, tables)
        for name, function in inspect.getmembers(module, inspect.isfunction)
        if function.__module__ == module.__name__
        and not name.startswith("_")
        and not inspect.getdoc(function)
    ]

    assert undocumented == []
