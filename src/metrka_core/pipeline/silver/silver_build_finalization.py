"""Finalize one Silver build and persist its publication decision."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from metrka_core.catalog.temporal_coverage import TemporalCoverageSpec
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.provenance import CodeProvenance
from metrka_core.pipeline.runtime_services import Clock
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.build_models import RebuildDecision
from metrka_core.pipeline.silver.fingerprints import SilverDatasetFingerprint
from metrka_core.pipeline.silver.postgres_publication_decision_unit_of_work import (
    PostgresSilverPublicationDecisionUnitOfWork,
)
from metrka_core.pipeline.silver.publication_decision_unit_of_work import (
    SilverPublicationDecisionCommand,
    SilverPublicationDecisionResult,
)
from metrka_core.pipeline.silver.silver_artifacts import write_silver_manifest
from metrka_core.pipeline.silver.version_period import VersionPeriod
from metrka_core.storage.checksums import sha256_file


@dataclass(frozen=True)
class SilverBuildFinalizationDeps:
    """Dependencies required to finalize one Silver build."""

    clock: Clock
    silver_store: SilverArtifactStore
    decision_uow: PostgresSilverPublicationDecisionUnitOfWork
    execution_log_store: ExecutionLogStore


@dataclass(frozen=True)
class SilverBuildFinalizationRequest:
    """Inputs required to finalize one Silver build."""

    dataset_name: str
    dataset_id: str
    pipeline_run_id: str
    silver_run_id: str

    bronze_file_id: str
    bronze_run_id: str
    source_file_name: str

    silver_build_id: str
    engine_release_id: str
    engine_hash: str
    processing_config_hash: str
    quality_config_hash: str

    version_period: VersionPeriod
    partition_key: str
    partition_value: str

    contract_path: Path
    contract_snapshot_path: Path
    contract_meta: Mapping[str, Any]

    staged_files: tuple[Path, ...]
    catalog_highlight_specs: tuple[dict[str, Any], ...]

    rebuild_decision: RebuildDecision
    code_provenance: CodeProvenance
    fingerprint: SilverDatasetFingerprint
    temporal_coverage_spec: TemporalCoverageSpec | None = None


@dataclass(frozen=True)
class SilverBuildFinalizationResult:
    """Result of finalizing one Silver build."""

    decision_result: SilverPublicationDecisionResult
    manifest_path: Path
    committed_files: tuple[Path, ...]
    observability_warning: str | None = None


@dataclass(frozen=True)
class _PreparedSilverBuild:
    """Durable files prepared before the database decision."""

    completed_at: datetime

    manifest_path: Path
    relative_manifest_path: str

    committed_files: tuple[Path, ...]

    input_byte_count: int
    output_byte_count: int
    output_hash: str


class SilverBuildFinalizationError(RuntimeError):
    """Raised when a Silver build could not be finalized."""


def _start_meta(request: SilverBuildFinalizationRequest) -> ExecutionStepMeta:
    contract_meta = dict(request.contract_meta)

    return ExecutionStepMeta(
        dataset_id=request.dataset_id,
        dataset_file_id=request.bronze_file_id,
        source_file_name=request.source_file_name,
        bronze_run_id=request.bronze_run_id,
        silver_run_id=request.silver_run_id,
        silver_build_id=request.silver_build_id,
        version_period=request.version_period.value.isoformat(),
        partition_key=request.partition_key,
        partition_value=request.partition_value,
        contract_hash=contract_meta.pop("contract_hash", None),
        contract_name=contract_meta.pop("contract_name", None),
        contract_path=contract_meta.pop("contract_path", None),
        contract_version=contract_meta.pop("contract_version", None),
        contract_snapshot_yaml_path=contract_meta.pop("contract_snapshot_yaml_path", None),
        contract_snapshot_json_path=contract_meta.pop("contract_snapshot_json_path", None),
        extra={
            "version_period_grain": request.version_period.grain,
            "version_period_source": request.version_period.source,
            "staged_files_count": len(request.staged_files),
            **contract_meta,
        },
    )


def _prepare_build(
    *, deps: SilverBuildFinalizationDeps, request: SilverBuildFinalizationRequest
) -> _PreparedSilverBuild:
    input_byte_count = sum(path.stat().st_size for path in request.staged_files)
    contract_definition_path = _required_contract_metadata(request.contract_meta, "contract_path")
    contract_snapshot_data_path = _required_contract_metadata(
        request.contract_meta, "contract_snapshot_yaml_path"
    )
    contract_version = _optional_contract_version(request.contract_meta)

    committed_files = tuple(
        deps.silver_store.commit_staged_files(
            run_id=request.silver_run_id,
            dataset_id=request.dataset_id,
            staged_files=list(request.staged_files),
        )
    )

    completed_at = deps.clock.now_utc()

    manifest_path, _manifest = write_silver_manifest(
        silver_store=deps.silver_store,
        dataset_id=request.dataset_id,
        silver_build_id=request.silver_build_id,
        engine_release_id=request.engine_release_id,
        processing_config_hash=request.processing_config_hash,
        quality_config_hash=request.quality_config_hash,
        build_signature=request.rebuild_decision.build_signature,
        rebuild_mode=request.rebuild_decision.mode.value,
        rebuild_reasons=[reason.value for reason in request.rebuild_decision.reasons],
        bronze_file_id=request.bronze_file_id,
        bronze_run_id=request.bronze_run_id,
        silver_run_id=request.silver_run_id,
        pipeline_run_id=request.pipeline_run_id,
        code_provenance=request.code_provenance,
        version_period=request.version_period,
        partition_key=request.partition_key,
        partition_value=request.partition_value,
        contract_path=request.contract_path,
        contract_definition_path=contract_definition_path,
        contract_snapshot_path=request.contract_snapshot_path,
        contract_snapshot_data_path=contract_snapshot_data_path,
        contract_version=contract_version,
        committed_files=list(committed_files),
        catalog_highlight_specs=[dict(spec) for spec in request.catalog_highlight_specs],
        fingerprint=request.fingerprint,
        temporal_coverage_spec=request.temporal_coverage_spec,
        created_at=completed_at,
    )

    relative_manifest_path = deps.silver_store.relative_path(manifest_path)

    output_byte_count = sum(path.stat().st_size for path in committed_files)

    output_hash = sha256_file(manifest_path)

    return _PreparedSilverBuild(
        completed_at=completed_at,
        manifest_path=manifest_path,
        relative_manifest_path=relative_manifest_path,
        committed_files=committed_files,
        input_byte_count=input_byte_count,
        output_byte_count=output_byte_count,
        output_hash=output_hash,
    )


def _required_contract_metadata(metadata: Mapping[str, Any], field_name: str) -> str:
    value = metadata.get(field_name)

    if not isinstance(value, str) or not value.strip():
        raise SilverBuildFinalizationError(
            f"Contract metadata field {field_name!r} must be a non-empty string"
        )

    return value.strip()


def _optional_contract_version(metadata: Mapping[str, Any]) -> str | None:
    value = metadata.get("contract_version")

    if value is None:
        return None

    if not isinstance(value, str):
        raise SilverBuildFinalizationError(
            "Contract metadata field 'contract_version' must be a string"
        )

    return value.strip() or None


def _decision_command(
    *, request: SilverBuildFinalizationRequest, prepared: _PreparedSilverBuild
) -> SilverPublicationDecisionCommand:
    return SilverPublicationDecisionCommand(
        dataset_id=request.dataset_id,
        bronze_file_id=request.bronze_file_id,
        silver_build_id=request.silver_build_id,
        engine_hash=request.engine_hash,
        quality_config_hash=request.quality_config_hash,
        version_period=request.version_period.value,
        partition_key=request.partition_key,
        partition_value=request.partition_value,
        manifest_path=prepared.relative_manifest_path,
        output_hash=prepared.output_hash,
        output_file_count=len(prepared.committed_files),
        output_byte_count=prepared.output_byte_count,
        completed_at=prepared.completed_at,
        fingerprint=request.fingerprint,
        marshal_meta={
            "stage": "silver",
            "bronze_run_id": request.bronze_run_id,
            "silver_run_id": request.silver_run_id,
            "silver_build_id": request.silver_build_id,
            "manifest_path": prepared.relative_manifest_path,
            "partition_key": request.partition_key,
            "partition_value": request.partition_value,
            "version_period_grain": request.version_period.grain,
            "version_period_source": request.version_period.source,
        },
    )


def _finish_meta(
    *,
    deps: SilverBuildFinalizationDeps,
    request: SilverBuildFinalizationRequest,
    prepared: _PreparedSilverBuild,
    decision_result: SilverPublicationDecisionResult,
) -> ExecutionStepMeta:
    verification_publication_id = (
        decision_result.verification.publication_id
        if decision_result.verification is not None
        else None
    )

    candidate_id = (
        decision_result.candidate.candidate_id if decision_result.candidate is not None else None
    )

    contract_meta = dict(request.contract_meta)

    return ExecutionStepMeta(
        dataset_id=request.dataset_id,
        dataset_file_id=request.bronze_file_id,
        source_file_name=request.source_file_name,
        bronze_run_id=request.bronze_run_id,
        silver_run_id=request.silver_run_id,
        silver_build_id=decision_result.completed_build.silver_build_id,
        version_period=request.version_period.value.isoformat(),
        partition_key=request.partition_key,
        partition_value=request.partition_value,
        contract_hash=contract_meta.pop("contract_hash", None),
        contract_name=contract_meta.pop("contract_name", None),
        contract_path=contract_meta.pop("contract_path", None),
        contract_version=contract_meta.pop("contract_version", None),
        contract_snapshot_yaml_path=contract_meta.pop("contract_snapshot_yaml_path", None),
        contract_snapshot_json_path=contract_meta.pop("contract_snapshot_json_path", None),
        input_file_count=len(request.staged_files),
        output_file_count=len(prepared.committed_files),
        input_byte_count=prepared.input_byte_count,
        output_byte_count=prepared.output_byte_count,
        manifest_path=prepared.relative_manifest_path,
        extra={
            "silver_build_status": decision_result.completed_build.status.value,
            "publication_decision_status": decision_result.decision.status.value,
            "publication_change_kind": decision_result.decision.change_kind.value,
            "baseline_publication_id": decision_result.decision.baseline_publication_id,
            "verification_publication_id": verification_publication_id,
            "publication_candidate_id": candidate_id,
            "version_period_grain": request.version_period.grain,
            "version_period_source": request.version_period.source,
            "committed_files": [
                deps.silver_store.relative_path(path) for path in prepared.committed_files
            ],
            **contract_meta,
        },
    )


def finalize_silver_build(
    *, deps: SilverBuildFinalizationDeps, request: SilverBuildFinalizationRequest
) -> SilverBuildFinalizationResult:
    """
    Finalize a Silver build without automatically publishing it.

    Files are first committed into immutable build storage. The
    database transaction then marks the build successful and records
    either a reproducibility verification or an approval candidate.
    """

    prepared: _PreparedSilverBuild | None = None
    decision_result: SilverPublicationDecisionResult | None = None

    try:
        with run_step(
            dataset=request.dataset_name,
            step="finalize_silver_build",
            layer="silver",
            run_id=request.silver_run_id,
            start_meta=_start_meta(request),
            execution_log_store=deps.execution_log_store,
        ) as step_context:
            prepared = _prepare_build(deps=deps, request=request)

            decision_result = deps.decision_uow.commit(
                _decision_command(request=request, prepared=prepared)
            )

            step_context.count_success(1)

            step_context.set_finish_meta(
                _finish_meta(
                    deps=deps, request=request, prepared=prepared, decision_result=decision_result
                )
            )

    except Exception as exc:
        if decision_result is None:
            raise SilverBuildFinalizationError(
                f"Silver build was not finalized for dataset_id={request.dataset_id}: {exc}"
            ) from exc

        if prepared is None:
            raise SilverBuildFinalizationError(
                "Silver publication decision exists without a prepared build "
                f"for dataset_id={request.dataset_id}"
            ) from exc

        return SilverBuildFinalizationResult(
            decision_result=decision_result,
            manifest_path=prepared.manifest_path,
            committed_files=prepared.committed_files,
            observability_warning=(f"{type(exc).__name__}: {exc}"),
        )

    if prepared is None or decision_result is None:
        raise SilverBuildFinalizationError(
            "Silver build finalization completed without "
            f"a result for dataset_id={request.dataset_id}"
        )

    return SilverBuildFinalizationResult(
        decision_result=decision_result,
        manifest_path=prepared.manifest_path,
        committed_files=prepared.committed_files,
    )
