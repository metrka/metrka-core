from __future__ import annotations

from datetime import UTC, datetime

import pytest

from metrka_core.pipeline.silver.build_models import (
    RebuildMode,
    RebuildReason,
    SilverBuild,
    SilverBuildStatus,
)
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
)
from metrka_core.pipeline.silver.rebuild_decision import (
    calculate_silver_build_signature,
    decide_silver_rebuild,
)

BASE = {
    "dataset_file_id": "file-1",
    "contract_hash": "a" * 64,
    "engine_release_id": "engine-1",
    "processing_config_hash": "b" * 64,
    "quality_config_hash": "c" * 64,
}


def _build(**changes: object) -> SilverBuild:
    values: dict[str, object] = {
        "silver_build_id": "build-1",
        "pipeline_run_id": "pipeline-1",
        "silver_run_id": "silver-1",
        "dataset_file_id": BASE["dataset_file_id"],
        "dataset_id": "dataset-1",
        "contract_hash": BASE["contract_hash"],
        "engine_release_id": BASE["engine_release_id"],
        "processing_config_hash": BASE["processing_config_hash"],
        "quality_config_hash": BASE["quality_config_hash"],
        "build_signature": calculate_silver_build_signature(**BASE),
        "fingerprint_version": 1,
        "logical_hash_algorithm": LOGICAL_DATA_HASH_ALGORITHM,
        "schema_hash_algorithm": SCHEMA_HASH_ALGORITHM,
        "status": SilverBuildStatus.SUCCEEDED,
        "rebuild_mode": RebuildMode.AUTOMATIC,
        "rebuild_reasons": (RebuildReason.INITIAL_BUILD,),
        "started_at": datetime(2026, 8, 13, tzinfo=UTC),
        "logical_data_hash": "d" * 64,
        "schema_hash": "e" * 64,
    }
    values.update(changes)
    return SilverBuild(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("dataset_file_id", "file-2"),
        ("contract_hash", "f" * 64),
        ("engine_release_id", "engine-2"),
        ("processing_config_hash", "1" * 64),
        ("quality_config_hash", "2" * 64),
    ],
)
def test_signature_changes_for_every_processing_identity_input(
    field: str, replacement: str
) -> None:
    changed = dict(BASE)
    changed[field] = replacement

    assert calculate_silver_build_signature(**changed) != (calculate_silver_build_signature(**BASE))


def test_matching_successful_build_is_skipped() -> None:
    matching = _build()

    decision = decide_silver_rebuild(
        **BASE,
        matching_successful_build=matching,
        latest_successful_build=matching,
        latest_build_attempt=matching,
    )

    assert not decision.required
    assert decision.matching_silver_build_id == "build-1"


def test_force_rebuild_is_manual_even_when_signature_matches() -> None:
    matching = _build()

    decision = decide_silver_rebuild(
        **BASE,
        matching_successful_build=matching,
        latest_successful_build=matching,
        latest_build_attempt=matching,
        force_rebuild=True,
    )

    assert decision.required
    assert decision.mode is RebuildMode.MANUAL
    assert decision.reasons == (RebuildReason.MANUAL_FORCE,)


def test_engine_and_quality_changes_are_reported() -> None:
    latest = _build(engine_release_id="old-engine", quality_config_hash="9" * 64)

    decision = decide_silver_rebuild(
        **BASE,
        matching_successful_build=None,
        latest_successful_build=latest,
        latest_build_attempt=latest,
    )

    assert decision.required
    assert decision.reasons == (
        RebuildReason.SILVER_ENGINE_CHANGED,
        RebuildReason.QUALITY_CONFIG_CHANGED,
    )
