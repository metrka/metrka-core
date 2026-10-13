from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from metrka_core.catalog.temporal_coverage import TemporalCoverageSpec
from metrka_core.pipeline.silver import silver_artifacts
from metrka_core.pipeline.silver.silver_artifacts import (
    contract_snapshot_metadata,
    snapshot_contract,
    write_silver_manifest,
)
from metrka_core.pipeline.silver.version_period import VersionPeriod
from metrka_core.storage.checksums import format_sha256_checksum
from metrka_core.storage.contract_store import LocalContractSnapshotStore
from metrka_core.storage.file_integrity import (
    FileIntegrityExpectation,
    Sha256WorkspaceFileIntegrityVerifier,
)


def _store(tmp_path: Path) -> LocalContractSnapshotStore:
    return LocalContractSnapshotStore(
        definition_root=tmp_path,
        data_root=tmp_path / "data",
        snapshots_root=tmp_path / "data" / "contracts",
    )


def test_snapshot_contract_is_content_addressed_and_immutable(tmp_path: Path) -> None:
    contract = tmp_path / "conf" / "contract.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("tables: {}\n", encoding="utf-8")

    paths = snapshot_contract(
        contract_store=_store(tmp_path), dataset_id="dataset-1", contract_path=contract
    )
    contract.write_text("tables:\n  revised: {}\n", encoding="utf-8")

    assert paths.yaml_path.read_text(encoding="utf-8") == "tables: {}\n"
    assert paths.json_path.exists()
    assert paths.yaml_path.parent.name.startswith("sha256=")


def test_snapshot_contract_rejects_corrupt_existing_snapshot(tmp_path: Path) -> None:
    contract = tmp_path / "conf" / "contract.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("tables: {}\n", encoding="utf-8")
    store = _store(tmp_path)
    paths = snapshot_contract(contract_store=store, dataset_id="dataset-1", contract_path=contract)
    paths.yaml_path.write_text("corrupt\n", encoding="utf-8")

    with pytest.raises(ValueError, match="checksum mismatch"):
        snapshot_contract(contract_store=store, dataset_id="dataset-1", contract_path=contract)


def test_contract_metadata_uses_owning_root_paths_for_split_roots(tmp_path: Path) -> None:
    definition_root = tmp_path / "definitions" / "example"
    data_root = tmp_path / "persistent-data" / "example"
    contract = definition_root / "conf" / "contract.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text("meta:\n  version: 1\ntables: {}\n", encoding="utf-8")
    store = LocalContractSnapshotStore(
        definition_root=definition_root, data_root=data_root, snapshots_root=data_root / "contracts"
    )

    metadata = contract_snapshot_metadata(
        contract_store=store, dataset_id="example.data", contract_path=contract
    )

    assert metadata["contract_path"] == "conf/contract.yaml"
    assert metadata["contract_snapshot_yaml_path"].startswith("contracts/example.data/sha256=")
    assert metadata["contract_snapshot_yaml_path"].endswith("/contract.yaml")
    assert metadata["contract_snapshot_json_path"].endswith("/contract.json")

    snapshot_contract(contract_store=store, dataset_id="example.data", contract_path=contract)
    integrity = Sha256WorkspaceFileIntegrityVerifier(workspace_root=data_root).inspect(
        FileIntegrityExpectation(
            artifact_kind="contract_snapshot",
            owner_id="publication-1",
            file_path=metadata["contract_snapshot_yaml_path"],
            expected_checksum=format_sha256_checksum(metadata["contract_hash"]),
        )
    )

    assert not integrity.failed


def test_contract_store_rejects_paths_outside_their_owning_roots(tmp_path: Path) -> None:
    store = _store(tmp_path / "workspace")

    with pytest.raises(ValueError, match="outside definition_root"):
        store.definition_relative_path(tmp_path / "unrelated" / "contract.yaml")

    with pytest.raises(ValueError, match="outside data_root"):
        store.snapshot_relative_path(tmp_path / "unrelated" / "snapshot.yaml")


def test_silver_manifest_uses_portable_contract_paths_without_resolving_physical_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    contract_path = tmp_path / "definitions" / "example" / "conf" / "contract.yaml"
    snapshot_path = tmp_path / "data" / "contracts" / "example" / "contract.yaml"
    contract_path.parent.mkdir(parents=True)
    snapshot_path.parent.mkdir(parents=True)
    contract_path.write_text("tables: {}\n", encoding="utf-8")
    snapshot_path.write_text("tables: {}\n", encoding="utf-8")
    silver_store = Mock()
    silver_store.tables_root = tmp_path / "data" / "files" / "silver" / "tables"
    silver_store.write_manifest.return_value = tmp_path / "manifest.json"
    code_provenance = Mock()
    code_provenance.to_dict.return_value = {}
    fingerprint = Mock()
    fingerprint.to_manifest_dict.return_value = {}
    expected_coverage = {
        "grain": "year",
        "source": {"table_key": "records", "column": "reporting_year"},
        "periods": [{"start": 2002, "end": 2025}],
        "minimum": 2002,
        "maximum": 2025,
        "distinct_value_count": 24,
    }
    monkeypatch.setattr(
        silver_artifacts, "calculate_temporal_coverage", lambda **_kwargs: expected_coverage
    )

    _, manifest = write_silver_manifest(
        silver_store=silver_store,
        dataset_id="example.data",
        silver_build_id="silver-build-1",
        engine_release_id="engine-release-1",
        processing_config_hash="processing-hash",
        quality_config_hash="quality-hash",
        build_signature="build-signature",
        rebuild_mode="full",
        rebuild_reasons=[],
        bronze_file_id="bronze-file-1",
        bronze_run_id="bronze-run-1",
        silver_run_id="silver-run-1",
        pipeline_run_id="pipeline-run-1",
        code_provenance=code_provenance,
        created_at=datetime(2026, 8, 19, tzinfo=UTC),
        version_period=VersionPeriod(value=date(2026, 1, 1), grain="year", source="test"),
        partition_key="year",
        partition_value="2026",
        contract_path=contract_path,
        contract_definition_path="conf/contract.yaml",
        contract_snapshot_path=snapshot_path,
        contract_snapshot_data_path="contracts/example/contract.yaml",
        contract_version="1",
        committed_files=[],
        catalog_highlight_specs=[],
        fingerprint=fingerprint,
        temporal_coverage_spec=TemporalCoverageSpec(
            table_key="records", column="reporting_year", grain="year"
        ),
    )

    assert manifest["contract"]["path"] == "conf/contract.yaml"
    assert manifest["contract"]["snapshot_path"] == "contracts/example/contract.yaml"
    assert manifest["contract"]["version"] == "1"
    assert manifest["catalog"]["temporal_coverage"] == expected_coverage
    silver_store.relative_path.assert_not_called()
