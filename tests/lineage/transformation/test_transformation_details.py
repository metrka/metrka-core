from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from metrka_core.lineage.transformation import (
    TRANSFORMATION_DETAILS_SCHEMA_ID,
    TRANSFORMATION_DETAILS_VALUE_ENCODING,
    TransformationDetailRow,
    TransformationObservation,
    write_transformation_details,
)
from metrka_core.transform.ops.text import normalize_values


def _observation(
    *,
    before_value: object = 10,
    after_value: object = "10",
    first_context: dict[str, object] | None = None,
    second_context: dict[str, object] | None = None,
) -> TransformationObservation:
    return TransformationObservation(
        operation="normalize_values",
        column_name="status",
        before_value=before_value,
        after_value=after_value,
        affected_row_count=2,
        record_details=True,
        detail_columns=("record_id", "amount"),
        detail_rows=(
            TransformationDetailRow(
                source_row_number=1,
                context=first_context or {"record_id": "a", "amount": Decimal("1.50")},
            ),
            TransformationDetailRow(
                source_row_number=3,
                context=second_context or {"amount": Decimal("2.00"), "record_id": "c"},
            ),
        ),
    )


def _write(destination: Path, observation: TransformationObservation) -> None:
    write_transformation_details(
        destination=destination,
        transformation_impact_id="impact-1",
        observation=observation,
        pipeline_run_id="pipeline-1",
        dataset_id="example.dataset",
        dataset_file_id="file-1",
        bronze_run_id="bronze-1",
        silver_run_id="silver-1",
        silver_build_id="build-1",
        table_key="example-table",
        source_file_name="source.csv",
        partition_key="version_period",
        partition_value="2026",
        version_period=date(2026, 1, 1),
        contract_hash="contract-sha256",
    )


def test_details_file_has_explicit_versioned_schema_and_tagged_values(tmp_path: Path) -> None:
    destination = tmp_path / "details.parquet"
    artifact = write_transformation_details(
        destination=destination,
        transformation_impact_id="impact-1",
        observation=_observation(),
        pipeline_run_id="pipeline-1",
        dataset_id="example.dataset",
        dataset_file_id="file-1",
        bronze_run_id="bronze-1",
        silver_run_id="silver-1",
        silver_build_id="build-1",
        table_key="example-table",
        source_file_name="source.csv",
        partition_key="version_period",
        partition_value="2026",
        version_period=date(2026, 1, 1),
        contract_hash="contract-sha256",
    )

    schema = pq.read_schema(destination)
    table = pq.read_table(destination)

    assert artifact.row_count == 2
    assert len(artifact.sha256) == 64
    assert artifact.schema_id == TRANSFORMATION_DETAILS_SCHEMA_ID
    assert artifact.value_encoding == TRANSFORMATION_DETAILS_VALUE_ENCODING
    assert schema.names == [
        "transformation_impact_id",
        "pipeline_run_id",
        "dataset_id",
        "dataset_file_id",
        "bronze_run_id",
        "silver_run_id",
        "silver_build_id",
        "table_key",
        "source_file_name",
        "partition_key",
        "partition_value",
        "version_period",
        "contract_hash",
        "operation",
        "column_name",
        "source_row_number",
        "context__amount",
        "context__record_id",
        "before_value",
        "after_value",
    ]
    assert schema.field("version_period").type == pa.date32()
    assert schema.field("source_row_number").type == pa.int64()
    assert schema.field("before_value").type == pa.string()
    assert schema.metadata == {
        b"metrka.schema_id": TRANSFORMATION_DETAILS_SCHEMA_ID.encode("utf-8"),
        b"metrka.value_encoding": TRANSFORMATION_DETAILS_VALUE_ENCODING.encode("utf-8"),
        b"metrka.context_columns": b'["amount","record_id"]',
        b"metrka.contract_hash": b"contract-sha256",
    }
    assert table.column("before_value").to_pylist() == ['["integer","10"]'] * 2
    assert table.column("after_value").to_pylist() == ['["string","10"]'] * 2
    assert table.column("context__amount").to_pylist() == [
        '["decimal","1.50"]',
        '["decimal","2.00"]',
    ]


def test_context_column_order_does_not_depend_on_dictionary_insertion_order(tmp_path: Path) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write(first, _observation())
    _write(
        second,
        _observation(
            first_context={"amount": Decimal("1.50"), "record_id": "a"},
            second_context={"record_id": "c", "amount": Decimal("2.00")},
        ),
    )

    assert pq.read_schema(first).equals(pq.read_schema(second), check_metadata=True)


def test_observation_rejects_context_that_does_not_match_declared_columns() -> None:
    with pytest.raises(ValueError, match=r"missing=\['record_id'\], unexpected=\['other'\]"):
        TransformationObservation(
            operation="normalize_values",
            column_name="status",
            before_value="old",
            after_value="new",
            affected_row_count=1,
            record_details=True,
            detail_columns=("record_id",),
            detail_rows=(TransformationDetailRow(source_row_number=1, context={"other": "value"}),),
        )


def test_writer_rejects_unknown_value_types_instead_of_inventing_a_schema(tmp_path: Path) -> None:
    destination = tmp_path / "details.parquet"

    with pytest.raises(TypeError, match="Unsupported canonical scalar value type"):
        _write(destination, _observation(before_value=object()))

    assert not destination.exists()


def test_normalize_values_carries_contract_detail_columns_into_evidence() -> None:
    source = pd.DataFrame(
        {
            "status": ["old", "unchanged"],
            "record_id": ["a", "b"],
            "amount": [Decimal("1.50"), Decimal("2.00")],
        }
    )
    rules = {
        "status": {
            "old": {
                "replace_with": "new",
                "reason": "Correct a known source value.",
                "record_details": True,
                "detail_columns": ["record_id", "amount"],
            }
        }
    }

    result = normalize_values(source, rules)
    observation = result.evidence[0]

    assert isinstance(observation, TransformationObservation)
    assert observation.detail_columns == ("record_id", "amount")
    assert observation.detail_rows[0].context == {"record_id": "a", "amount": Decimal("1.50")}
