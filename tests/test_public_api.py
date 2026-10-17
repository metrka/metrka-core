"""Smoke tests for the supported metrka-core API surface."""

from __future__ import annotations

import re
from pathlib import Path

_PUBLIC_SYMBOL_REFERENCE_HEADING = "## Public symbol reference"
_PUBLIC_SYMBOL_PATTERN = re.compile(r"`([A-Za-z_][A-Za-z0-9_]*)`")


def _documented_public_symbols() -> dict[str, tuple[str, str]]:
    document_path = Path(__file__).resolve().parents[1] / "PUBLIC_API.md"
    document = document_path.read_text(encoding="utf-8")

    if document.count(_PUBLIC_SYMBOL_REFERENCE_HEADING) != 1:
        raise AssertionError("PUBLIC_API.md must contain one public symbol reference section")

    section = document.split(_PUBLIC_SYMBOL_REFERENCE_HEADING, maxsplit=1)[1]
    section = section.split("\n## ", maxsplit=1)[0]
    documented: dict[str, tuple[str, str]] = {}

    for line in section.splitlines():
        if not line.startswith("| `"):
            continue

        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 3:
            raise AssertionError(f"Invalid public symbol reference row: {line}")

        symbol_cell, category, contract = cells
        match = _PUBLIC_SYMBOL_PATTERN.fullmatch(symbol_cell)
        if match is None:
            raise AssertionError(f"Invalid public symbol cell: {symbol_cell}")

        symbol = match.group(1)
        if symbol in documented:
            raise AssertionError(f"Duplicate public symbol reference: {symbol}")
        if not category:
            raise AssertionError(f"Public symbol has no category: {symbol}")
        if len(contract) < 20:
            raise AssertionError(f"Public symbol contract is not substantive: {symbol}")

        documented[symbol] = (category, contract)

    if not documented:
        raise AssertionError("Public symbol reference contains no symbol rows")

    return documented


def test_pipeline_api_exports() -> None:
    import metrka_core.api as api

    expected = {
        "Clock",
        "ContractSnapshotFailure",
        "ContractSnapshotReader",
        "ContractSnapshotReadError",
        "PipelineRegistry",
        "PipelineBootstrapOptions",
        "PipelineRunIdGenerator",
        "PipelineRunResult",
        "PipelineRunState",
        "MainTemplateError",
        "ProfileError",
        "PublicationManifestFailure",
        "PublicationManifestReader",
        "PublicationManifestReadError",
        "RunName",
        "RuntimeServices",
        "RuntimeEnvironment",
        "BronzeRunIdGenerator",
        "DatasetFileIdGenerator",
        "DatasetIdentity",
        "DatasetIdentityError",
        "WorkspaceLocation",
        "WorkspaceLocationResolver",
        "WorkspaceName",
        "WorkspacePlacement",
        "SilverBuildIdGenerator",
        "WorkspaceInitializationResult",
        "WorkspaceImportResult",
        "WorkspaceExportContentPolicyError",
        "WorkspaceExportPolicyViolation",
        "WorkspaceExportIntegrityError",
        "WorkspaceExportResult",
        "WorkspaceExportVerificationResult",
        "WorkspaceValidationResult",
        "create_contract_snapshot_reader",
        "create_core_registry",
        "create_publication_manifest_reader",
        "create_workspace_location_resolver",
        "execute_configured_pipeline",
        "export_workspace",
        "initialize_workspace",
        "load_contract",
        "load_main_config",
        "import_workspace",
        "open_pipeline_context",
        "parse_dataset_id",
        "run_pipeline",
        "validate_workspace",
        "verify_workspace_export",
    }

    assert set(api.__all__) == expected

    for name in expected:
        assert hasattr(api, name)


def test_every_pipeline_api_export_has_a_reference_contract() -> None:
    import metrka_core.api as api

    documented = _documented_public_symbols()

    assert set(documented) == set(api.__all__)


def test_action_extension_exports() -> None:
    import metrka_core.extensions.action as action

    expected = {
        "ActionDefinition",
        "ActionDependencyResolver",
        "ActionOutcome",
        "ActionRuntime",
        "ArtifactRef",
        "PipelineContext",
        "PipelineRegistry",
        "PipelineRunState",
    }

    assert set(action.__all__) == expected

    for name in expected:
        assert hasattr(action, name)


def test_acquisition_extension_exports() -> None:
    import metrka_core.extensions.acquisition as acquisition

    expected = {
        "AcquisitionDeps",
        "ActionRuntime",
        "AssetExtractor",
        "Clock",
        "ExecutionStepMeta",
        "LandedAsset",
        "MonotonicClock",
        "SourceCapture",
        "run_step",
    }

    assert set(acquisition.__all__) == expected

    for name in expected:
        assert hasattr(acquisition, name)


def test_source_schema_extension_exports() -> None:
    import metrka_core.extensions.source_schema as source_schema

    expected = {
        "BronzeArtifactStore",
        "BronzeBatchResult",
        "BronzeIngestResult",
        "FileMarshal",
        "ParsedSourceSchema",
        "SOURCE_SCHEMA_HASH_ALGORITHM",
        "SourceConfig",
        "SourceSchemaField",
        "SourceSchemaFieldBinding",
        "SourceSchemaStore",
        "compare_source_schema_fields",
    }

    assert set(source_schema.__all__) == expected

    for name in expected:
        assert hasattr(source_schema, name)
