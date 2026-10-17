"""Behavioural contract tests for the pipeline-run lifecycle."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import cast

import pytest

from metrka_core.datasets.dataset_identity import RunName
from metrka_core.pipeline.composition.lifecycle import pipeline_run
from metrka_core.pipeline.composition.runtime import RuntimeComposition
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.context import PipelineContext
from metrka_core.pipeline.provenance import CodeProvenance, GitCodeRevision
from metrka_core.pipeline.silver.engine_models import SilverEngineIdentity, SilverEnginePolicy

STARTED_AT = datetime(2026, 8, 14, 10, 0, tzinfo=UTC)

FINISHED_AT = datetime(2026, 8, 14, 10, 5, tzinfo=UTC)


class SequenceClock:
    """Return predefined timestamps in order."""

    def __init__(self, *values: datetime) -> None:
        self._values = iter(values)

    def now_utc(self) -> datetime:
        return next(self._values)


class RecordingPipelineRunStore:
    """Record lifecycle calls without PostgreSQL."""

    def __init__(self) -> None:
        self.started: list[dict[str, object]] = []
        self.finished: list[dict[str, object]] = []

    def start_pipeline_run(
        self,
        *,
        pipeline_run_id: str,
        run_name: str,
        config_name: str,
        code_provenance: CodeProvenance,
        started_at: datetime,
    ) -> None:
        self.started.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "run_name": run_name,
                "config_name": config_name,
                "code_provenance": code_provenance,
                "started_at": started_at,
            }
        )

    def finish_pipeline_run(
        self,
        *,
        pipeline_run_id: str,
        status: str,
        finished_at: datetime,
        error: dict[str, object] | None = None,
    ) -> None:
        self.finished.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "status": status,
                "finished_at": finished_at,
                "error": error,
            }
        )


def _runtime() -> RuntimeComposition:
    revision = GitCodeRevision(
        repository="test", commit_sha="a" * 40, branch="main", package_version="0.1.0"
    )

    engine_identity = SilverEngineIdentity(
        release_hash="a" * 64,
        engine_hash="b" * 64,
        engine_fingerprint_version=1,
        runtime_hash="c" * 64,
        runtime_fingerprint_version=1,
        component_hashes={"test": "d" * 64},
        runtime_versions={"python": "3.14"},
    )

    return RuntimeComposition(
        pipeline_run_id="pipeline_test",
        code_provenance=CodeProvenance(
            metrka_core=revision, dataset_repository=revision, dirty=False
        ),
        runtime_environment=RuntimeEnvironment.DEVELOPMENT,
        silver_engine_identity=engine_identity,
        silver_engine_policy=SilverEnginePolicy.ALLOW_CANDIDATE,
    )


def _pipeline_context() -> PipelineContext:
    """
    Return an opaque context sentinel.

    pipeline_run() only returns the context to its caller. It does not
    inspect its fields, so constructing the complete application object
    graph would make this lifecycle test less focused.
    """

    return cast(PipelineContext, object())


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, CodeProvenance):
        return value.to_dict()

    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _canonical_receipt(store: RecordingPipelineRunStore) -> bytes:
    """
    Serialize the complete fake-store state canonically.

    Sorting keys and removing formatting whitespace means equal logical
    records produce exactly equal bytes.
    """

    payload = {"started": store.started, "finished": store.finished}

    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default).encode(
        "utf-8"
    )


def _record_successful_run() -> RecordingPipelineRunStore:
    store = RecordingPipelineRunStore()
    context = _pipeline_context()

    with pipeline_run(
        context=context,
        runtime=_runtime(),
        pipeline_runs=store,
        clock=SequenceClock(STARTED_AT, FINISHED_AT),
        run_name=RunName("test-workspace"),
        config_name="main.yaml",
    ) as yielded_context:
        assert yielded_context is context

    return store


def _record_failed_run() -> RecordingPipelineRunStore:
    store = RecordingPipelineRunStore()

    with (
        pytest.raises(ValueError, match="invalid input"),
        pipeline_run(
            context=_pipeline_context(),
            runtime=_runtime(),
            pipeline_runs=store,
            clock=SequenceClock(STARTED_AT, FINISHED_AT),
            run_name=RunName("test-workspace"),
            config_name="main.yaml",
        ),
    ):
        raise ValueError("invalid input")

    return store


def test_successful_pipeline_lifecycle_is_deterministic() -> None:
    first = _record_successful_run()
    second = _record_successful_run()

    expected_provenance = _runtime().code_provenance

    expected_started = [
        {
            "pipeline_run_id": "pipeline_test",
            "run_name": "test-workspace",
            "config_name": "main.yaml",
            "code_provenance": expected_provenance,
            "started_at": STARTED_AT,
        }
    ]

    expected_finished = [
        {
            "pipeline_run_id": "pipeline_test",
            "status": "success",
            "finished_at": FINISHED_AT,
            "error": None,
        }
    ]

    assert isinstance(first.started[0]["code_provenance"], CodeProvenance)

    assert first.started == expected_started
    assert first.finished == expected_finished

    assert second.started == expected_started
    assert second.finished == expected_finished

    assert _canonical_receipt(first) == _canonical_receipt(second)


def test_failed_pipeline_lifecycle_is_deterministic() -> None:
    first = _record_failed_run()
    second = _record_failed_run()

    expected_finished = [
        {
            "pipeline_run_id": "pipeline_test",
            "status": "failed",
            "finished_at": FINISHED_AT,
            "error": {"type": "ValueError", "message": "invalid input"},
        }
    ]

    assert first.finished == expected_finished
    assert second.finished == expected_finished

    assert _canonical_receipt(first) == _canonical_receipt(second)
