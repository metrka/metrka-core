"""Tests for typed execution-step audit metadata."""

from __future__ import annotations

from pathlib import Path

import pytest

from metrka_core.observability.execution_step_meta import ExecutionStepMeta


def test_canonical_fields_are_serialized_with_explicit_names() -> None:
    meta = ExecutionStepMeta(
        dataset_id="adult-lead",
        dataset_file_id="file-123",
        source_capture_id="capture-123",
        table_key="county",
        input_row_count=10,
        extra={"quality_gate": "passed"},
    )

    assert meta.to_dict() == {
        "quality_gate": "passed",
        "dataset_id": "adult-lead",
        "dataset_file_id": "file-123",
        "source_capture_id": "capture-123",
        "table_key": "county",
        "input_row_count": 10,
    }


def test_finish_metadata_overlays_start_metadata() -> None:
    start = ExecutionStepMeta(
        dataset_id="adult-lead",
        dataset_file_id="candidate-file",
        extra={"phase": "started", "source": "landing"},
    )
    finish = ExecutionStepMeta(
        dataset_file_id="registered-file", output_row_count=100, extra={"phase": "finished"}
    )

    assert start.merged_with(finish) == ExecutionStepMeta(
        dataset_id="adult-lead",
        dataset_file_id="registered-file",
        output_row_count=100,
        extra={"phase": "finished", "source": "landing"},
    )


def test_extra_cannot_shadow_queryable_fields() -> None:
    with pytest.raises(ValueError, match="reserved fields.*dataset_file_id"):
        ExecutionStepMeta(extra={"dataset_file_id": "wrong-place"})


@pytest.mark.parametrize("count", [-1, True, 1.5])
def test_counts_must_be_non_negative_integers(count: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        ExecutionStepMeta(input_row_count=count)  # type: ignore[arg-type]


def test_extra_rejects_non_json_values() -> None:
    with pytest.raises(TypeError, match="non-JSON value"):
        ExecutionStepMeta(extra={"path": Path("C:/data/file.csv")})


def test_serialized_mapping_is_validated_at_the_boundary() -> None:
    with pytest.raises(TypeError, match="dataset_file_id must be a string"):
        ExecutionStepMeta.from_mapping({"dataset_file_id": 42})
