"""Select Silver candidates before expensive dataset and Bronze preparation."""

from __future__ import annotations

from dataclasses import dataclass, replace

from metrka_core.metadata.file_marshal_models import SilverCandidateFile
from metrka_core.pipeline.silver.build_models import SilverBuild
from metrka_core.pipeline.silver.candidate_dataset_preparation import SilverDatasetContractIdentity
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.rebuild_decision import calculate_silver_build_signature
from metrka_core.pipeline.silver.silver_artifacts import contract_snapshot_metadata
from metrka_core.pipeline.silver.task_models import SilverTaskConfig
from metrka_core.storage.config_store import ConfigStore
from metrka_core.storage.contract_store import ContractSnapshotStore


@dataclass(frozen=True)
class SilverCandidateSelectionDeps:
    """Dependencies used by the cheap batch selection boundary."""

    config_store: ConfigStore
    contract_store: ContractSnapshotStore
    silver_build_store: PostgresSilverBuildStore


@dataclass(frozen=True)
class SelectedSilverCandidate:
    """One candidate with its deterministic identity already resolved."""

    dataset_file_id: str
    dataset_id: str
    bronze_run_id: str
    task: SilverTaskConfig
    contract_identity: SilverDatasetContractIdentity
    build_signature: str
    matching_successful_build: SilverBuild | None = None


@dataclass(frozen=True)
class SilverCandidateSelection:
    """Candidates split into cheap skips and work that must continue."""

    pending: tuple[SelectedSilverCandidate, ...]
    skipped: tuple[SelectedSilverCandidate, ...]


def select_silver_candidates(
    *,
    deps: SilverCandidateSelectionDeps,
    records: tuple[SilverCandidateFile, ...],
    task_map: dict[str, SilverTaskConfig],
    engine_release_id: str,
    quality_config_hash: str,
    force_rebuild: bool,
) -> SilverCandidateSelection:
    """Resolve each contract once and find all matching builds in one query."""

    contract_identities: dict[str, SilverDatasetContractIdentity] = {}
    candidates: list[SelectedSilverCandidate] = []

    for record in records:
        dataset_file_id = record.dataset_file_id
        dataset_id = record.dataset_id
        bronze_run_id = record.bronze_run_id

        task = task_map.get(dataset_id)
        if task is None:
            raise KeyError(dataset_id)

        identity = contract_identities.get(dataset_id)
        if identity is None:
            contract_path = deps.config_store.path(name=task.yaml_contract_name)
            identity = SilverDatasetContractIdentity(
                contract_path=contract_path,
                contract_meta=contract_snapshot_metadata(
                    contract_store=deps.contract_store,
                    dataset_id=dataset_id,
                    contract_path=contract_path,
                ),
            )
            contract_identities[dataset_id] = identity

        build_signature = calculate_silver_build_signature(
            dataset_file_id=dataset_file_id,
            contract_hash=identity.contract_meta["contract_hash"],
            engine_release_id=engine_release_id,
            processing_config_hash=task.processing_config_hash,
            quality_config_hash=quality_config_hash,
        )

        candidates.append(
            SelectedSilverCandidate(
                dataset_file_id=dataset_file_id,
                dataset_id=dataset_id,
                bronze_run_id=bronze_run_id,
                task=task,
                contract_identity=identity,
                build_signature=build_signature,
            )
        )

    matching_builds = deps.silver_build_store.find_successful_by_signatures(
        {candidate.build_signature for candidate in candidates}
    )

    pending: list[SelectedSilverCandidate] = []
    skipped: list[SelectedSilverCandidate] = []

    for candidate in candidates:
        resolved = replace(
            candidate, matching_successful_build=matching_builds.get(candidate.build_signature)
        )

        if resolved.matching_successful_build is not None and not force_rebuild:
            skipped.append(resolved)
        else:
            pending.append(resolved)

    return SilverCandidateSelection(pending=tuple(pending), skipped=tuple(skipped))
