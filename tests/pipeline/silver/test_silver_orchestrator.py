from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from metrka_core.metadata.file_marshal_models import SilverCandidateFile
from metrka_core.observability.execution_events import ExecutionCounts, StepFinishedEvent
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.provenance import CodeProvenance, GitCodeRevision
from metrka_core.pipeline.silver import silver_orchestrator
from metrka_core.pipeline.silver.candidate_dataset_preparation import (
    PreparedSilverDataset,
    SilverDatasetContractIdentity,
)
from metrka_core.pipeline.silver.candidate_selection import (
    SelectedSilverCandidate,
    SilverCandidateSelection,
)
from metrka_core.pipeline.silver.dependencies import SilverProcessDeps
from metrka_core.pipeline.silver.process_models import (
    SilverCandidateOutcome,
    SilverCandidateOutcomeStatus,
    SilverDatasetFailure,
    SilverFailureStage,
    SilverProcessingError,
)
from metrka_core.pipeline.silver.task_models import SilverTaskConfig

from .publication.fakes import RecordingExecutionLogStore


def test_task_config_remains_available_from_orchestrator_module() -> None:
    assert silver_orchestrator.SilverTaskConfig is SilverTaskConfig


class FakeCandidateFileStore:
    def __init__(self, records: tuple[SilverCandidateFile, ...]) -> None:
        self.records = records
        self.requested_dataset_id: str | None = None

    def get_silver_candidate_files(
        self, *, dataset_id: str | None = None
    ) -> tuple[SilverCandidateFile, ...]:
        self.requested_dataset_id = dataset_id
        return self.records


@dataclass(frozen=True)
class FakeInputs:
    file_marshal_store: FakeCandidateFileStore


@dataclass(frozen=True)
class FakeEvidence:
    execution_log_store: RecordingExecutionLogStore


@dataclass(frozen=True)
class FakeProcessDeps:
    inputs: FakeInputs
    evidence: FakeEvidence


@dataclass(frozen=True)
class FakeCandidateDeps:
    selection: object
    dataset_preparation: object
    engine_release_id: str = "engine-test"
    quality_config_hash: str = "q" * 64


def _runtime() -> ActionRuntime:
    revision = GitCodeRevision(
        repository="metrka-core", commit_sha="abc123", branch="test", package_version="1.0.0"
    )
    return ActionRuntime(
        pipeline_run_id="pipeline-test",
        dataset_name="test_workspace",
        code_provenance=CodeProvenance(
            metrka_core=revision, dataset_repository=revision, dirty=False
        ),
    )


def _task(dataset_id: str) -> SilverTaskConfig:
    return SilverTaskConfig(
        dataset_id=dataset_id,
        yaml_contract_name=f"{dataset_id}.yaml",
        partition_key="version_period",
        version_period_discovery_func=lambda *_args: None,  # type: ignore[arg-type,return-value]
        processing_config_hash="a" * 64,
    )


def _deps(
    *, records: tuple[SilverCandidateFile, ...], execution_logs: RecordingExecutionLogStore
) -> SilverProcessDeps:
    return cast(
        SilverProcessDeps,
        FakeProcessDeps(
            inputs=FakeInputs(file_marshal_store=FakeCandidateFileStore(records)),
            evidence=FakeEvidence(execution_log_store=execution_logs),
        ),
    )


def _selected_candidate(
    *, record: SilverCandidateFile, task: SilverTaskConfig, matching: bool = False
) -> SelectedSilverCandidate:
    dataset_id = record.dataset_id
    return SelectedSilverCandidate(
        dataset_file_id=record.dataset_file_id,
        dataset_id=dataset_id,
        bronze_run_id=record.bronze_run_id,
        task=task,
        contract_identity=SilverDatasetContractIdentity(
            contract_path=Path(f"{dataset_id}.yaml"), contract_meta={"contract_hash": "c" * 64}
        ),
        build_signature="s" * 64,
        matching_successful_build=cast(Any, SimpleNamespace(silver_build_id="silver-existing"))
        if matching
        else None,
    )


def _prepared_dataset(candidate: SelectedSilverCandidate) -> PreparedSilverDataset:
    return PreparedSilverDataset(
        dataset_id=candidate.dataset_id,
        contract_path=candidate.contract_identity.contract_path,
        contract_meta=candidate.contract_identity.contract_meta,
        contract_snapshot_path=Path("contracts") / candidate.contract_identity.contract_path.name,
        configured_tables={"table": {}},
    )


def test_queue_aggregates_candidate_outcomes_at_batch_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = (
        SilverCandidateFile(
            dataset_file_id="file-1", dataset_id="example.finalized", bronze_run_id="bronze-1"
        ),
        SilverCandidateFile(
            dataset_file_id="file-2", dataset_id="example.skipped", bronze_run_id="bronze-2"
        ),
    )
    execution_logs = RecordingExecutionLogStore()
    candidate_deps = FakeCandidateDeps(selection=object(), dataset_preparation=object())
    seen_requests: list[object] = []

    finalized = _selected_candidate(record=records[0], task=_task("example.finalized"))
    skipped = _selected_candidate(record=records[1], task=_task("example.skipped"), matching=True)

    monkeypatch.setattr(
        silver_orchestrator, "build_silver_candidate_execution_deps", lambda _deps: candidate_deps
    )
    monkeypatch.setattr(
        silver_orchestrator,
        "select_silver_candidates",
        lambda **_kwargs: SilverCandidateSelection(pending=(finalized,), skipped=(skipped,)),
    )
    monkeypatch.setattr(
        silver_orchestrator,
        "prepare_silver_dataset",
        lambda **_kwargs: _prepared_dataset(finalized),
    )

    def fake_process_one_candidate(*, runtime: object, deps: object, request: Any) -> Any:
        assert runtime is not None
        assert deps is candidate_deps
        seen_requests.append(request)

        if request.dataset_id == "example.skipped":
            return SilverCandidateOutcome(
                dataset_id=request.dataset_id, status=SilverCandidateOutcomeStatus.SKIPPED
            )

        return SilverCandidateOutcome(
            dataset_id=request.dataset_id,
            status=SilverCandidateOutcomeStatus.FINALIZED,
            warnings=("cleanup warning",),
        )

    monkeypatch.setattr(silver_orchestrator, "process_one_candidate", fake_process_one_candidate)

    result = silver_orchestrator.process_silver_queue(
        runtime=_runtime(),
        deps=_deps(records=records, execution_logs=execution_logs),
        tasks=[_task("example.finalized"), _task("example.skipped")],
        target_dataset_id=None,
        force_rebuild=True,
    )

    assert result.finalized_dataset_ids == ("example.finalized",)
    assert result.skipped_dataset_ids == ("example.skipped",)
    assert result.warnings == ("cleanup warning",)
    assert len(seen_requests) == 1

    finished = [event for event in execution_logs.records if isinstance(event, StepFinishedEvent)]

    assert finished[-1].counts == ExecutionCounts(success=1, failed=0, skipped=1, blocked=0)


def test_queue_ignores_bronze_files_of_datasets_from_other_workspaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = (
        SilverCandidateFile(
            dataset_file_id="file-1", dataset_id="other_workspace.data", bronze_run_id="bronze-1"
        ),
        SilverCandidateFile(
            dataset_file_id="file-2", dataset_id="example.own", bronze_run_id="bronze-2"
        ),
    )
    selected_records: list[tuple[SilverCandidateFile, ...]] = []
    candidate_deps = FakeCandidateDeps(selection=object(), dataset_preparation=object())

    monkeypatch.setattr(
        silver_orchestrator, "build_silver_candidate_execution_deps", lambda _deps: candidate_deps
    )

    def record_selection(**kwargs: Any) -> SilverCandidateSelection:
        selected_records.append(tuple(kwargs["records"]))
        return SilverCandidateSelection(pending=(), skipped=())

    monkeypatch.setattr(silver_orchestrator, "select_silver_candidates", record_selection)

    silver_orchestrator.process_silver_queue(
        runtime=_runtime(),
        deps=_deps(records=records, execution_logs=RecordingExecutionLogStore()),
        tasks=[_task("example.own")],
    )

    assert selected_records == [(records[1],)]


def test_queue_rejects_a_target_dataset_this_workspace_does_not_configure() -> None:
    with pytest.raises(SilverProcessingError) as error:
        silver_orchestrator.process_silver_queue(
            runtime=_runtime(),
            deps=_deps(records=(), execution_logs=RecordingExecutionLogStore()),
            tasks=[_task("example.own")],
            target_dataset_id="other_workspace.data",
        )

    assert error.value.failure.error_code == "SILVER_TASK_NOT_CONFIGURED"


def test_queue_counts_structured_candidate_failure_once(monkeypatch: pytest.MonkeyPatch) -> None:
    records = (
        SilverCandidateFile(
            dataset_file_id="file-1", dataset_id="example.failed", bronze_run_id="bronze-1"
        ),
    )
    execution_logs = RecordingExecutionLogStore()

    candidate_deps = FakeCandidateDeps(selection=object(), dataset_preparation=object())
    selected = _selected_candidate(record=records[0], task=_task("example.failed"))

    monkeypatch.setattr(
        silver_orchestrator, "build_silver_candidate_execution_deps", lambda _deps: candidate_deps
    )
    monkeypatch.setattr(
        silver_orchestrator,
        "select_silver_candidates",
        lambda **_kwargs: SilverCandidateSelection(pending=(selected,), skipped=()),
    )
    monkeypatch.setattr(
        silver_orchestrator, "prepare_silver_dataset", lambda **_kwargs: _prepared_dataset(selected)
    )

    def fail_candidate(**_kwargs: object) -> SilverCandidateOutcome:
        raise SilverProcessingError(
            SilverDatasetFailure(
                dataset_id="example.failed",
                stage=SilverFailureStage.TABLE_BUILD,
                error_code="SILVER_TABLE_BUILD_FAILED",
                message="build failed",
            )
        )

    monkeypatch.setattr(silver_orchestrator, "process_one_candidate", fail_candidate)

    with pytest.raises(SilverProcessingError):
        silver_orchestrator.process_silver_queue(
            runtime=_runtime(),
            deps=_deps(records=records, execution_logs=execution_logs),
            tasks=[_task("example.failed")],
        )

    finished = [event for event in execution_logs.records if isinstance(event, StepFinishedEvent)]

    assert finished[-1].status == "failed"
    assert finished[-1].counts.failed == 1


def test_queue_prepares_one_dataset_once_for_multiple_pending_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = (
        SilverCandidateFile(
            dataset_file_id="file-1", dataset_id="example.dataset", bronze_run_id="bronze-1"
        ),
        SilverCandidateFile(
            dataset_file_id="file-2", dataset_id="example.dataset", bronze_run_id="bronze-2"
        ),
    )
    task = _task("example.dataset")
    candidates = tuple(_selected_candidate(record=record, task=task) for record in records)
    execution_logs = RecordingExecutionLogStore()
    candidate_deps = FakeCandidateDeps(selection=object(), dataset_preparation=object())
    preparation_calls: list[str] = []

    monkeypatch.setattr(
        silver_orchestrator, "build_silver_candidate_execution_deps", lambda _deps: candidate_deps
    )
    monkeypatch.setattr(
        silver_orchestrator,
        "select_silver_candidates",
        lambda **_kwargs: SilverCandidateSelection(pending=candidates, skipped=()),
    )

    def fake_prepare(**kwargs: Any) -> PreparedSilverDataset:
        preparation_calls.append(cast(str, kwargs["dataset_id"]))
        return _prepared_dataset(candidates[0])

    monkeypatch.setattr(silver_orchestrator, "prepare_silver_dataset", fake_prepare)
    monkeypatch.setattr(
        silver_orchestrator,
        "process_one_candidate",
        lambda **kwargs: SilverCandidateOutcome(
            dataset_id=kwargs["request"].dataset_id, status=SilverCandidateOutcomeStatus.FINALIZED
        ),
    )

    result = silver_orchestrator.process_silver_queue(
        runtime=_runtime(), deps=_deps(records=records, execution_logs=execution_logs), tasks=[task]
    )

    assert preparation_calls == ["example.dataset"]
    assert result.finalized_dataset_ids == ("example.dataset", "example.dataset")
