from __future__ import annotations

import json
from datetime import UTC, date, datetime
from itertools import count
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from metrka_core.pipeline.silver.silver_builder import build_silver_table
from metrka_core.pipeline.silver.version_period import VersionPeriod
from metrka_core.quality.config import QualityConfig
from metrka_core.storage.silver_store import LocalSilverArtifactStore

SILVER_PROCESSED_AT = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


def _silver_store(tmp_path: Path) -> LocalSilverArtifactStore:
    return LocalSilverArtifactStore(
        workspace_root=tmp_path,
        silver_root=tmp_path / "data" / "files" / "silver",
        current_root=tmp_path / "data" / "current",
    )


def _contract(tmp_path: Path) -> Path:
    contract = tmp_path / "conf" / "contract.yaml"
    contract.parent.mkdir(parents=True)
    contract.write_text(
        """
tables:
  people:
    columns:
      id:
        rename_to: id
        cast_to: string
      name:
        rename_to: name
        cast_to: string
    canonical_order:
      - id
      - name
""".lstrip(),
        encoding="utf-8",
    )
    return contract


def _reconciliation_contract(tmp_path: Path) -> Path:
    contract = tmp_path / "conf" / "reconciliation.yaml"
    contract.parent.mkdir(parents=True, exist_ok=True)

    contract.write_text(
        """
tables:
  people:
    columns:
      County:
        rename_to: geography_name
        cast_to: string
      Count:
        rename_to: licensed_bed_count
        cast_to: int
      reporting_year:
        rename_to: reporting_year
        cast_to: int
      cid_id:
        rename_to: indicator_id
        cast_to: string

    parent_child_reconciliation:
      group_by:
        - reporting_year
        - indicator_id

      parent:
        column: geography_name
        equals: Florida

      measure_column: licensed_bed_count

      residual:
        label_column: geography_name
        label_value: Unallocated

      remove_parent: true
      negative_difference: fail

    canonical_order:
      - geography_name
      - licensed_bed_count
      - reporting_year
      - indicator_id
""".lstrip(),
        encoding="utf-8",
    )

    return contract


def _quality_config() -> QualityConfig:
    return QualityConfig()


def _build(
    *,
    tmp_path: Path,
    source: Path,
    input_format: str = "csv",
    contract_path: Path | None = None,
    transformation_impact_store: MagicMock | None = None,
    transformation_impact_ids: MagicMock | None = None,
    quality_store: MagicMock | None = None,
):
    resolved_impact_store = (
        transformation_impact_store if transformation_impact_store is not None else MagicMock()
    )

    resolved_quality_store = quality_store if quality_store is not None else MagicMock()

    if transformation_impact_ids is None:
        resolved_impact_ids = MagicMock()
        impact_number = count(1)

        resolved_impact_ids.new_transformation_impact_id.side_effect = lambda: (
            f"impact-test-{next(impact_number)}"
        )
    else:
        resolved_impact_ids = transformation_impact_ids

    return build_silver_table(
        dataset_name="people",
        silver_store=_silver_store(tmp_path),
        dataset_id="people.dataset",
        bronze_file_id="bronze-file-1",
        bronze_run_id="bronze-run-1",
        silver_build_id="silver-build-1",
        version_period=VersionPeriod(value=date(2025, 1, 1), grain="year", source="column:year"),
        partition_key="version_period",
        partition_value="2025",
        source_file_name=source.name,
        bronze_ingested_at=datetime(2026, 8, 13, tzinfo=UTC),
        silver_processed_at=SILVER_PROCESSED_AT,
        input_file_path=source,
        cfg_path=(contract_path if contract_path is not None else _contract(tmp_path)),
        table_key="people",
        execution_log_store=MagicMock(),
        quality_store=resolved_quality_store,
        transformation_impact_store=resolved_impact_store,
        transformation_impact_ids=resolved_impact_ids,
        run_id="silver-run-1",
        pipeline_run_id="pipeline-1",
        quality_config=_quality_config(),
        input_format=input_format,
        output_formats="csv",
    )


def test_builder_writes_data_preview_and_business_fingerprint(tmp_path: Path) -> None:
    source = tmp_path / "people.csv"
    source.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    result = _build(tmp_path=tmp_path, source=source)

    data_path = next(path for path in result.staged_paths if path.suffix == ".csv")
    preview_path = next(path for path in result.staged_paths if path.suffix == ".json")
    output = pd.read_csv(data_path, dtype=str)
    preview = json.loads(preview_path.read_text(encoding="utf-8"))

    assert data_path.is_file()
    assert preview_path.is_file()
    assert result.fingerprint.table_key == "people"
    assert result.fingerprint.row_count == 2
    assert result.fingerprint.column_count == 2
    assert output.columns.tolist() == ["id", "name"]
    assert output.to_dict(orient="records") == [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]
    assert preview["columns"] == ["id", "name"]
    assert preview["rows"] == [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]


def test_builder_rejects_unsupported_input_format(tmp_path: Path) -> None:
    source = tmp_path / "people.json"
    source.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported input_format"):
        _build(tmp_path=tmp_path, source=source, input_format="json")


def test_builder_assigns_explicit_identity_and_time_to_impacts(tmp_path: Path) -> None:
    source = tmp_path / "people.csv"
    source.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    impact_store = MagicMock()
    impact_ids = MagicMock()
    impact_number = count(1)

    impact_ids.new_transformation_impact_id.side_effect = lambda: (
        f"impact-fixed-{next(impact_number)}"
    )

    _build(
        tmp_path=tmp_path,
        source=source,
        transformation_impact_store=impact_store,
        transformation_impact_ids=impact_ids,
    )

    impact_store.insert_many.assert_called_once()

    impacts = impact_store.insert_many.call_args.args[0]

    assert impacts
    assert all(impact.recorded_at == SILVER_PROCESSED_AT for impact in impacts)
    assert all(impact.transformation_impact_id.startswith("impact-fixed-") for impact in impacts)
    assert impact_ids.new_transformation_impact_id.call_count == len(impacts)


def test_builder_quality_evidence_uses_workspace_relative_paths(tmp_path: Path) -> None:
    source = tmp_path / "people.csv"
    source.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

    quality_store = MagicMock()

    result = _build(tmp_path=tmp_path, source=source, quality_store=quality_store)

    quality_records = [
        call.args[0] for call in quality_store.insert_quality_check_run.call_args_list
    ]
    output_record = next(
        record for record in quality_records if record["check_id"].endswith(".output_files_created")
    )

    silver_store = _silver_store(tmp_path)
    data_paths = [path for path in result.staged_paths if path.suffix == ".csv"]
    expected_paths = [silver_store.relative_path(path) for path in data_paths]

    assert output_record["actual"]["output_files"] == expected_paths
    assert str(tmp_path) not in str(output_record["actual"])


def test_builder_persists_parent_child_reconciliation_evidence(tmp_path: Path) -> None:
    source = tmp_path / "people.csv"
    source.write_text(
        (
            "County,Count,reporting_year,cid_id\n"
            "Florida,35,2002,0321\n"
            "Alachua,10,2002,0321\n"
            "Baker,20,2002,0321\n"
        ),
        encoding="utf-8",
    )

    impact_store = MagicMock()

    result = _build(
        tmp_path=tmp_path,
        source=source,
        contract_path=_reconciliation_contract(tmp_path),
        transformation_impact_store=impact_store,
    )

    impact_store.insert_many.assert_called_once()
    impacts = impact_store.insert_many.call_args.args[0]

    reconciliation = next(
        impact for impact in impacts if impact.operation == "parent_child_reconciliation"
    )

    assert reconciliation.column_name == "licensed_bed_count"
    assert reconciliation.affected_row_count == 1
    assert reconciliation.meta["evidence_kind"] == ("parent_child_reconciliation")
    assert reconciliation.meta["metrics"]["parent_total"] == 35
    assert reconciliation.meta["metrics"]["child_total"] == 30
    assert reconciliation.meta["metrics"]["difference"] == 5
    assert reconciliation.meta["metrics"]["group"] == {
        "reporting_year": 2002,
        "indicator_id": "0321",
    }

    data_path = next(path for path in result.staged_paths if path.suffix == ".csv")
    output = pd.read_csv(data_path, dtype=str)

    assert output.columns.tolist() == [
        "geography_name",
        "licensed_bed_count",
        "reporting_year",
        "indicator_id",
    ]
    assert output["geography_name"].tolist() == ["Alachua", "Baker", "Unallocated"]
    assert output["licensed_bed_count"].tolist() == ["10", "20", "5"]
