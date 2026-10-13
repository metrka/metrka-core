"""Prepare dataset-level Silver contract state once per batch."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from metrka_core.catalog.postgres_dataset_catalog import PostgresDatasetCatalogStore
from metrka_core.metadata.postgres_contract_metadata import PostgresContractMetadataStore
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.silver.silver_artifacts import register_contract_snapshot
from metrka_core.storage.contract_store import ContractSnapshotStore
from metrka_core.transform.validation import ContractValidationError, validate_contract_file


@dataclass(frozen=True)
class SilverDatasetContractIdentity:
    """Contract path and content identity shared by every candidate for a dataset."""

    contract_path: Path
    contract_meta: dict[str, str]


@dataclass(frozen=True)
class PreparedSilverDataset:
    """Validated dataset-level state reused by all candidates in one batch."""

    dataset_id: str
    contract_path: Path
    contract_meta: dict[str, str]
    contract_snapshot_path: Path
    configured_tables: dict[str, Any]


@dataclass(frozen=True)
class SilverDatasetPreparationDeps:
    """Dependencies required for one dataset-level preparation."""

    contract_store: ContractSnapshotStore
    contract_metadata_store: PostgresContractMetadataStore
    dataset_catalog_store: PostgresDatasetCatalogStore
    execution_log_store: ExecutionLogStore


def _contract_step_meta(
    *,
    dataset_id: str,
    silver_run_id: str,
    identity: SilverDatasetContractIdentity,
    include_zero_counts: bool = False,
) -> ExecutionStepMeta:
    contract_meta = identity.contract_meta
    return ExecutionStepMeta(
        dataset_id=dataset_id,
        silver_run_id=silver_run_id,
        contract_hash=contract_meta.get("contract_hash"),
        contract_name=contract_meta.get("contract_name"),
        contract_path=contract_meta.get("contract_path"),
        contract_version=contract_meta.get("contract_version"),
        contract_snapshot_yaml_path=contract_meta.get("contract_snapshot_yaml_path"),
        contract_snapshot_json_path=contract_meta.get("contract_snapshot_json_path"),
        input_file_count=0 if include_zero_counts else None,
        output_file_count=0 if include_zero_counts else None,
        input_byte_count=0 if include_zero_counts else None,
        output_byte_count=0 if include_zero_counts else None,
    )


def prepare_silver_dataset(
    *,
    deps: SilverDatasetPreparationDeps,
    dataset_name: str,
    dataset_id: str,
    silver_run_id: str,
    identity: SilverDatasetContractIdentity,
) -> PreparedSilverDataset:
    """Validate, register and snapshot one contract exactly once per dataset."""

    with run_step(
        dataset=dataset_name,
        step="validate_silver_contract",
        layer="silver",
        run_id=silver_run_id,
        start_meta=_contract_step_meta(
            dataset_id=dataset_id, silver_run_id=silver_run_id, identity=identity
        ),
        execution_log_store=deps.execution_log_store,
    ) as validation_context:
        contract = validate_contract_file(identity.contract_path)

        try:
            deps.dataset_catalog_store.register_dataset_catalog_from_contract(
                dataset_id=dataset_id,
                contract_hash=identity.contract_meta["contract_hash"],
                contract=contract,
            )
        except ValueError as error:
            raise ContractValidationError(str(error)) from error

        raw_tables = contract.get("tables")
        if not isinstance(raw_tables, dict):
            raise ContractValidationError("Silver contract tables must be a mapping")

        configured_tables: dict[str, Any] = dict(raw_tables)
        if not configured_tables:
            raise ContractValidationError(
                f"No tables are configured in YAML contract {identity.contract_path.name}"
            )

        validation_context.count_success(len(configured_tables))
        validation_context.set_finish_meta(
            _contract_step_meta(
                dataset_id=dataset_id,
                silver_run_id=silver_run_id,
                identity=identity,
                include_zero_counts=True,
            )
        )

    contract_snapshot_path = register_contract_snapshot(
        contract_store=deps.contract_store,
        contract_metadata_store=deps.contract_metadata_store,
        dataset_name=dataset_name,
        dataset_id=dataset_id,
        contract_path=identity.contract_path,
        contract_meta=identity.contract_meta,
    )

    return PreparedSilverDataset(
        dataset_id=dataset_id,
        contract_path=identity.contract_path,
        contract_meta=identity.contract_meta,
        contract_snapshot_path=contract_snapshot_path,
        configured_tables=configured_tables,
    )
