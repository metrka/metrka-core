"""Gate functions: which checks run, what evidence is written, when a gate blocks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import pandas as pd

from metrka_core.quality.config import parse_quality_config
from metrka_core.quality.gates import (
    BronzeOutput,
    LandedFile,
    RunIds,
    SilverTable,
    check_bronze_output,
    check_landed_file,
    check_silver_input,
    check_silver_output,
)
from metrka_core.quality.models import QualityOutputFile

IDS = RunIds(dataset_id="ws.stream", run_id="run-1", pipeline_run_id="pipe-1")


class RecordingStore:
    def __init__(self) -> None:
        self.definitions: list[dict[str, Any]] = []
        self.runs: list[dict[str, Any]] = []

    def upsert_quality_check_definition(self, record: dict[str, Any]) -> None:
        self.definitions.append(record)

    def insert_quality_check_run(self, record: dict[str, Any]) -> None:
        self.runs.append(record)

    def status_by_id(self) -> dict[str, str]:
        return {run["check_id"]: run["status"] for run in self.runs}


def _landed(path: Path) -> LandedFile:
    return LandedFile(path=path, sha256="a" * 64, fingerprint={path.name: {}})


def _silver(frame: pd.DataFrame, tmp_path: Path) -> SilverTable:
    output = tmp_path / "beds.parquet"
    output.write_bytes(b"data")
    return SilverTable(
        table_key="beds",
        frame=frame,
        expected_columns=tuple(frame.columns),
        source_file_name="beds.csv",
        output_files=(
            QualityOutputFile(local_path=output, workspace_relative_path="silver/beds.parquet"),
        ),
    )


def test_landed_csv_gets_only_the_generic_file_checks(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    path.write_text("a\n1\n", encoding="utf-8")
    store = RecordingStore()

    result = check_landed_file(_landed(path), ids=IDS, store=store)

    assert not result.failed
    assert store.status_by_id() == {
        "ws.stream.pre_bronze.file_not_empty": "passed",
        "ws.stream.pre_bronze.sha256_recorded": "passed",
    }


def test_landed_zip_and_xlsx_get_format_checks(tmp_path: Path) -> None:
    archive = tmp_path / "data.zip"
    with ZipFile(archive, "w") as zip_file:
        zip_file.writestr("data.csv", "a\n1\n")
    workbook = tmp_path / "data.xlsx"
    pd.DataFrame({"a": [1]}).to_excel(workbook, index=False)

    zip_store, xlsx_store = RecordingStore(), RecordingStore()
    check_landed_file(_landed(archive), ids=IDS, store=zip_store)
    check_landed_file(
        LandedFile(
            path=workbook, sha256="a" * 64, fingerprint={}, xlsx_sheet_name=0, xlsx_header_row=0
        ),
        ids=IDS,
        store=xlsx_store,
    )

    assert {run["check_id"].rsplit(".", 1)[-1] for run in zip_store.runs} == {
        "file_not_empty",
        "sha256_recorded",
        "zip_crc_valid",
        "payload_fingerprint_recorded",
    }
    assert {run["check_id"].rsplit(".", 1)[-1] for run in xlsx_store.runs} == {
        "file_not_empty",
        "sha256_recorded",
        "xlsx_package_integrity",
        "xlsx_has_data_rows",
    }


def test_evidence_rows_carry_gate_ids_and_relative_names_only(tmp_path: Path) -> None:
    path = tmp_path / "data.csv"
    path.write_text("a\n", encoding="utf-8")
    store = RecordingStore()

    check_landed_file(_landed(path), ids=IDS, store=store)

    run = store.runs[0]
    definition = store.definitions[0]
    assert run["details"] == {"source_file_name": "data.csv", "quality_gate": "pre_bronze"}
    assert run["actual"]["file_name"] == "data.csv"
    assert run["pipeline_run_id"] == "pipe-1"
    assert run["step_id"] == "quality_ws_stream_pre_bronze_file_not_empty"
    assert definition["layer"] == "bronze"
    assert definition["code_ref"] == "metrka_core.quality.checks.files.file_not_empty"
    assert definition["description"] == "The file exists and has at least 1 byte(s)."
    assert str(tmp_path) not in json.dumps([store.runs, store.definitions])


def test_unchanged_archive_with_no_new_output_does_not_block() -> None:
    """A ZIP whose members did not change produces no Bronze output; that is not a failure."""

    store = RecordingStore()

    result = check_bronze_output(BronzeOutput(files=(), required=False), ids=IDS, store=store)

    assert not result.failed
    assert result.skipped_count == 1
    assert store.status_by_id() == {"ws.stream.post_bronze.output_files_created": "skipped"}


def test_check_that_raises_is_recorded_as_blocking_error(tmp_path: Path) -> None:
    store = RecordingStore()
    config = parse_quality_config(
        {"version": 1, "tables": {"beds": {"columns": {"missing": ["not_null"]}}}}
    )

    result = check_silver_output(
        _silver(pd.DataFrame({"a": [1]}), tmp_path), config=config, ids=IDS, store=store
    )

    assert result.failed
    assert result.error_count == 1
    assert store.status_by_id()["ws.stream.post_silver.beds.missing.not_null"] == "error"
    assert result.error_message is not None
    assert "KeyError" in result.error_message


def test_silver_input_allows_extra_columns(tmp_path: Path) -> None:
    store = RecordingStore()
    table = SilverTable(
        table_key="beds",
        frame=pd.DataFrame({"County": ["A"], "Extra": [1]}),
        expected_columns=("County",),
        source_file_name="beds.csv",
    )

    result = check_silver_input(table, ids=IDS, store=store)

    assert not result.failed
    assert store.runs[0]["details"]["quality_gate"] == "pre_silver"


def test_silver_output_runs_yaml_rules_with_their_severity(tmp_path: Path) -> None:
    frame = pd.DataFrame({"county": ["A", "B"], "count": [1, -1], "rate": [5.0, 500.0]})
    config = parse_quality_config(
        {
            "version": 1,
            "tables": {
                "beds": {
                    "unique": ["county"],
                    "columns": {
                        "count": [{"min": 0}],
                        "rate": [{"max": 100, "severity": "warning"}],
                    },
                }
            },
        }
    )
    store = RecordingStore()

    result = check_silver_output(_silver(frame, tmp_path), config=config, ids=IDS, store=store)

    assert store.status_by_id() == {
        "ws.stream.post_silver.beds.has_rows": "passed",
        "ws.stream.post_silver.beds.columns_match": "passed",
        "ws.stream.post_silver.beds.output_files_created": "passed",
        "ws.stream.post_silver.beds.unique": "passed",
        "ws.stream.post_silver.beds.count.min": "failed",
        "ws.stream.post_silver.beds.rate.max": "failed",
    }
    assert result.failed
    assert result.blocked_count == 1
    assert result.failed_check_ids == [
        "ws.stream.post_silver.beds.count.min",
        "ws.stream.post_silver.beds.rate.max",
    ]
    rate_definition = store.definitions[-1]
    assert rate_definition["severity"] == "warning"
    assert rate_definition["target"] == "table_key=beds;column=rate"
    assert rate_definition["default_params"] == {"column": "rate", "maximum": 100}
    descriptions = {
        definition["check_id"].rsplit(".", 1)[-1]: definition["description"]
        for definition in store.definitions
    }
    assert descriptions["unique"] == "No two rows share the same values in county."
    assert descriptions["min"] == "Every present value is at least 0."
    assert descriptions["max"] == "Every present value is at most 100."
    assert descriptions["has_rows"] == "The table has at least 1 row(s)."
    assert "``" not in " ".join(descriptions.values())


def test_rules_for_other_tables_do_not_run(tmp_path: Path) -> None:
    config = parse_quality_config(
        {"version": 1, "tables": {"other": {"columns": {"x": ["not_null"]}}}}
    )
    store = RecordingStore()

    check_silver_output(
        _silver(pd.DataFrame({"a": [1]}), tmp_path), config=config, ids=IDS, store=store
    )

    assert len(store.runs) == 3
