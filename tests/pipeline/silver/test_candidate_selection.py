from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from metrka_core.metadata.file_marshal_models import SilverCandidateFile
from metrka_core.pipeline.silver import candidate_selection
from metrka_core.pipeline.silver.build_models import SilverBuild
from metrka_core.pipeline.silver.candidate_selection import (
    SilverCandidateSelectionDeps,
    select_silver_candidates,
)
from metrka_core.pipeline.silver.task_models import SilverTaskConfig


class RecordingConfigStore:
    def __init__(self, contract_path: Path) -> None:
        self.contract_path = contract_path
        self.requested_names: list[str] = []

    def path(self, *, name: str) -> Path:
        self.requested_names.append(name)
        return self.contract_path


class RecordingBuildStore:
    def __init__(self) -> None:
        self.requested_signatures: set[str] = set()
        self.matching: dict[str, SilverBuild] = {}

    def find_successful_by_signatures(self, build_signatures: set[str]) -> dict[str, SilverBuild]:
        self.requested_signatures = set(build_signatures)
        return dict(self.matching)


def _task() -> SilverTaskConfig:
    return SilverTaskConfig(
        dataset_id="example.dataset",
        yaml_contract_name="example.yaml",
        partition_key="version_period",
        version_period_discovery_func=lambda *_args: None,  # type: ignore[arg-type,return-value]
        processing_config_hash="p" * 64,
    )


def _records() -> tuple[SilverCandidateFile, ...]:
    return (
        SilverCandidateFile(
            dataset_file_id="file-1", dataset_id="example.dataset", bronze_run_id="bronze-1"
        ),
        SilverCandidateFile(
            dataset_file_id="file-2", dataset_id="example.dataset", bronze_run_id="bronze-2"
        ),
    )


def test_selection_resolves_contract_once_and_queries_builds_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_store = RecordingConfigStore(tmp_path / "example.yaml")
    build_store = RecordingBuildStore()
    metadata_calls: list[str] = []

    def fake_contract_metadata(**kwargs: Any) -> dict[str, str]:
        metadata_calls.append(cast(str, kwargs["dataset_id"]))
        return {"contract_hash": "c" * 64}

    monkeypatch.setattr(candidate_selection, "contract_snapshot_metadata", fake_contract_metadata)

    result = select_silver_candidates(
        deps=SilverCandidateSelectionDeps(
            config_store=cast(Any, config_store),
            contract_store=cast(Any, object()),
            silver_build_store=cast(Any, build_store),
        ),
        records=_records(),
        task_map={"example.dataset": _task()},
        engine_release_id="engine-test",
        quality_config_hash="q" * 64,
        force_rebuild=False,
    )

    assert len(result.pending) == 2
    assert result.skipped == ()
    assert config_store.requested_names == ["example.yaml"]
    assert metadata_calls == ["example.dataset"]
    assert build_store.requested_signatures == {
        candidate.build_signature for candidate in result.pending
    }


def test_selection_skips_matching_build_unless_force_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_store = RecordingConfigStore(tmp_path / "example.yaml")
    build_store = RecordingBuildStore()

    monkeypatch.setattr(
        candidate_selection,
        "contract_snapshot_metadata",
        lambda **_kwargs: {"contract_hash": "c" * 64},
    )

    initial = select_silver_candidates(
        deps=SilverCandidateSelectionDeps(
            config_store=cast(Any, config_store),
            contract_store=cast(Any, object()),
            silver_build_store=cast(Any, build_store),
        ),
        records=_records()[:1],
        task_map={"example.dataset": _task()},
        engine_release_id="engine-test",
        quality_config_hash="q" * 64,
        force_rebuild=False,
    )
    signature = initial.pending[0].build_signature
    build_store.matching = {
        signature: cast(
            SilverBuild,
            SimpleNamespace(silver_build_id="silver-existing", build_signature=signature),
        )
    }

    skipped = select_silver_candidates(
        deps=SilverCandidateSelectionDeps(
            config_store=cast(Any, config_store),
            contract_store=cast(Any, object()),
            silver_build_store=cast(Any, build_store),
        ),
        records=_records()[:1],
        task_map={"example.dataset": _task()},
        engine_release_id="engine-test",
        quality_config_hash="q" * 64,
        force_rebuild=False,
    )
    forced = select_silver_candidates(
        deps=SilverCandidateSelectionDeps(
            config_store=cast(Any, config_store),
            contract_store=cast(Any, object()),
            silver_build_store=cast(Any, build_store),
        ),
        records=_records()[:1],
        task_map={"example.dataset": _task()},
        engine_release_id="engine-test",
        quality_config_hash="q" * 64,
        force_rebuild=True,
    )

    assert len(skipped.skipped) == 1
    assert skipped.pending == ()
    assert len(forced.pending) == 1
    assert forced.skipped == ()
