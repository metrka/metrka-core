from __future__ import annotations

import pytest

from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.pipeline.silver.task_factory import build_silver_tasks


def _source_config(*, outputs: object, temporal_coverage: object | None = None) -> SourceConfig:
    silver: dict[str, object] = {
        "partition_by": "version_period",
        "input": {"format": "csv", "options": {}},
        "version_period": {"strategy": "source_last_modified", "grain": "day"},
        "outputs": outputs,
    }

    if temporal_coverage is not None:
        silver["temporal_coverage"] = temporal_coverage

    return SourceConfig(
        workspace_name="example",
        streams={
            "records": StreamConfig(
                name="records",
                official_filename="records.csv",
                yaml_contract_name="records.yaml",
                extra={"silver": silver},
            )
        },
    )


def test_build_silver_tasks_normalizes_supported_output_formats() -> None:
    tasks = build_silver_tasks(source_config=_source_config(outputs=[" CSV ", "parquet"]))

    assert len(tasks) == 1
    assert tasks[0].output_formats == ["csv", "parquet"]


def test_build_silver_tasks_rejects_unsupported_output_format() -> None:
    with pytest.raises(RuntimeError) as error:
        build_silver_tasks(source_config=_source_config(outputs=["parquet", "json"]))

    assert str(error.value) == (
        "silver.outputs contains unsupported formats for stream records: "
        "['json']; supported: ['csv', 'parquet']"
    )


@pytest.mark.parametrize("outputs", [[], [""], ["csv", 1], "csv"])
def test_build_silver_tasks_rejects_invalid_output_container(outputs: object) -> None:
    with pytest.raises(
        RuntimeError, match="silver.outputs must be a non-empty list of strings for stream records"
    ):
        build_silver_tasks(source_config=_source_config(outputs=outputs))


def test_build_silver_tasks_resolves_temporal_coverage() -> None:
    task = build_silver_tasks(
        source_config=_source_config(
            outputs=["parquet"],
            temporal_coverage={"table": "records", "column": "reporting_year", "grain": "year"},
        )
    )[0]

    assert task.temporal_coverage is not None
    assert task.temporal_coverage.to_config_dict() == {
        "table": "records",
        "column": "reporting_year",
        "grain": "year",
    }


def test_build_silver_tasks_rejects_invalid_temporal_coverage() -> None:
    with pytest.raises(RuntimeError, match="Invalid silver.temporal_coverage"):
        build_silver_tasks(
            source_config=_source_config(
                outputs=["parquet"],
                temporal_coverage={"table": "records", "column": "reported_at", "grain": "quarter"},
            )
        )
