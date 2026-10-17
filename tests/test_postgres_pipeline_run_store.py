"""Tests for the PostgreSQL pipeline-run persistence mapping."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from metrka_core.datasets.dataset_identity import RunName
from metrka_core.observability import postgres_stores
from metrka_core.observability.postgres_stores import PostgresPipelineRunStore
from metrka_core.pipeline.provenance import CodeProvenance, GitCodeRevision


def test_start_pipeline_run_serializes_typed_provenance_at_postgres_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    store = PostgresPipelineRunStore(session)

    core_revision = GitCodeRevision(
        repository="metrka-core", commit_sha="a" * 40, branch="main", package_version="1.1.0.dev0"
    )
    dataset_revision = GitCodeRevision(
        repository="metrka-datasets", commit_sha="b" * 40, branch="main", package_version="1.0.0"
    )
    code_provenance = CodeProvenance(
        metrka_core=core_revision, dataset_repository=dataset_revision, dirty=False
    )

    expected_payload = {
        "metrka_core": {
            "repository": "metrka-core",
            "commit_sha": "a" * 40,
            "branch": "main",
            "package_version": "1.1.0.dev0",
        },
        "dataset_repository": {
            "repository": "metrka-datasets",
            "commit_sha": "b" * 40,
            "branch": "main",
            "package_version": "1.0.0",
        },
        "dirty": False,
    }

    captured_jsonb_values: list[object] = []
    jsonb_parameter = object()

    def capture_jsonb(value: object) -> object:
        captured_jsonb_values.append(value)
        return jsonb_parameter

    monkeypatch.setattr(postgres_stores, "to_jsonb", capture_jsonb)

    started_at = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)

    store.start_pipeline_run(
        pipeline_run_id="pipeline_test",
        run_name=RunName("adult-lead"),
        config_name="main.yaml",
        code_provenance=code_provenance,
        started_at=started_at,
    )

    assert captured_jsonb_values == [expected_payload]

    query, parameters = cursor.execute.call_args.args

    assert "INSERT INTO logs.pipeline_runs" in query
    assert parameters == ("pipeline_test", "adult-lead", "main.yaml", started_at, jsonb_parameter)
