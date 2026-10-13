from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from metrka_core.datasets.source_config import SourceConfig, StreamConfig
from metrka_core.pipeline.bronze.asset_ingestion import (
    _group_landed_assets_by_stream,
    ingest_landed_assets,
)
from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataColumn,
    FilenameMetadataConfig,
)
from metrka_core.pipeline.bronze.models import BronzeIngestResult
from metrka_core.pipeline.bronze.xlsx_row_assembly import BronzeAssemblyConfig, XlsxReadConfig
from metrka_core.pipeline.models import LandedAsset
from metrka_core.storage.bronze_store import LocalBronzeArtifactStore


def _asset(tmp_path: Path, filename: str) -> LandedAsset:
    return LandedAsset(
        stream_name="county",
        path=tmp_path / filename,
        source_url="manual_upload",
        source_capture_id="capture-1",
    )


def test_groups_multiple_landed_assets_into_one_stream_batch(tmp_path: Path) -> None:
    assets = [
        _asset(tmp_path, "cid0321__single-year__default__all__2025.xlsx"),
        _asset(tmp_path, "cid0321__single-year__default__all__2002.xlsx"),
    ]

    grouped = _group_landed_assets_by_stream(assets)

    assert tuple(grouped) == ("county",)
    assert [asset.path.name for asset in grouped["county"]] == [
        "cid0321__single-year__default__all__2002.xlsx",
        "cid0321__single-year__default__all__2025.xlsx",
    ]


def test_ingests_multiple_xlsx_assets_as_one_stream_batch(tmp_path: Path) -> None:
    from metrka_core.pipeline.bronze.asset_ingestion import ingest_landed_assets

    filename_metadata = FilenameMetadataConfig(
        regex=(
            r"^cid(?P<cid_id>[0-9]+)__"
            r"(?P<year_breakdown>[^_]+)__"
            r"(?P<group_dimension>[^_]+)__"
            r"(?P<group_value>[^_]+)__"
            r"(?P<year>[0-9]{4})\.xlsx$"
        ),
        columns={
            "cid_id": FilenameMetadataColumn(from_group="cid_id", value_type="string"),
            "reporting_year": FilenameMetadataColumn(from_group="year", value_type="integer"),
        },
        member_key=("cid_id", "reporting_year"),
    )

    source_config = SourceConfig(
        workspace_name="fl_healthcharts",
        streams={
            "county": StreamConfig(
                name="county",
                official_filename="cid0321__*.xlsx",
                filename_metadata=filename_metadata,
                bronze_assembly=BronzeAssemblyConfig(
                    strategy="xlsx_rows", output_filename="county.csv", read_config=XlsxReadConfig()
                ),
            )
        },
    )

    deps = MagicMock()
    deps.source_config = source_config

    assets = [
        _asset(tmp_path, "cid0321__single-year__default__all__2002.xlsx"),
        _asset(tmp_path, "cid0321__single-year__default__all__2025.xlsx"),
    ]

    ingest_result = BronzeIngestResult(
        dataset_file_id="dataset-file-1",
        dataset_id="fl_healthcharts.county",
        source_hash="batch-hash",
        bronze_run_id="bronze-run-1",
        is_new=True,
    )

    with patch(
        "metrka_core.pipeline.bronze.asset_ingestion._ingest_xlsx_asset_batch",
        return_value=ingest_result,
    ) as ingest_batch:
        result = ingest_landed_assets(runtime=MagicMock(), deps=deps, assets=assets)

    ingest_batch.assert_called_once()

    called_assets = ingest_batch.call_args.kwargs["assets"]

    assert tuple(asset.path.name for asset in called_assets) == (
        "cid0321__single-year__default__all__2002.xlsx",
        "cid0321__single-year__default__all__2025.xlsx",
    )
    assert result.by_stream == {"county": ingest_result}
    assert result.new_count == 1
    assert result.duplicate_count == 0


def test_persists_multiple_xlsx_assets_as_one_bronze_file(tmp_path: Path) -> None:
    first = tmp_path / "cid0321__single-year__default__all__2002.xlsx"
    second = tmp_path / "cid0321__single-year__default__all__2025.xlsx"

    pd.DataFrame([{"County": "Florida", "Count": 10}]).to_excel(first, index=False)

    pd.DataFrame([{"County": "Florida", "Count": 12}]).to_excel(second, index=False)

    filename_metadata = FilenameMetadataConfig(
        regex=(
            r"^cid(?P<cid_id>[0-9]+)__"
            r"(?P<year_breakdown>[^_]+)__"
            r"(?P<group_dimension>[^_]+)__"
            r"(?P<group_value>[^_]+)__"
            r"(?P<year>[0-9]{4})\.xlsx$"
        ),
        columns={
            "cid_id": FilenameMetadataColumn(from_group="cid_id", value_type="string"),
            "reporting_year": FilenameMetadataColumn(from_group="year", value_type="integer"),
        },
        member_key=("cid_id", "reporting_year"),
    )

    source_config = SourceConfig(
        workspace_name="fl_healthcharts",
        streams={
            "county": StreamConfig(
                name="county",
                official_filename="cid0321__*.xlsx",
                filename_metadata=filename_metadata,
                bronze_assembly=BronzeAssemblyConfig(
                    strategy="xlsx_rows", output_filename="county.csv", read_config=XlsxReadConfig()
                ),
            )
        },
    )

    bronze_store = LocalBronzeArtifactStore(
        workspace_root=tmp_path,
        bronze_root=tmp_path / "data" / "files" / "bronze",
        current_root=tmp_path / "data" / "current",
    )

    deps = MagicMock()
    deps.source_config = source_config
    deps.bronze_store = bronze_store
    deps.execution_logs = MagicMock()
    deps.quality_checks = MagicMock()
    deps.file_marshal_store = MagicMock()

    deps.bronze_run_ids.new_bronze_run_id.return_value = "bronze-run-1"
    deps.dataset_file_ids.new_dataset_file_id.return_value = "dataset-file-1"
    deps.clock.now_utc.return_value = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    deps.marshal.get_by_hash.return_value = None

    runtime = MagicMock()
    runtime.dataset_name = "fl_healthcharts"
    runtime.pipeline_run_id = "pipeline-1"

    result = ingest_landed_assets(
        runtime=runtime,
        deps=deps,
        assets=[
            LandedAsset(
                stream_name="county",
                path=second,
                source_url="manual_upload",
                source_capture_id="capture-1",
            ),
            LandedAsset(
                stream_name="county",
                path=first,
                source_url="manual_upload",
                source_capture_id="capture-1",
            ),
        ],
    )

    bronze_file = bronze_store.run_dir(run_id="bronze-run-1") / "county.csv"

    assert bronze_file.is_file()

    written = pd.read_csv(bronze_file)

    assert written["reporting_year"].tolist() == [2002, 2025]
    assert result.by_stream["county"].dataset_file_id == "dataset-file-1"
    assert result.by_stream["county"].bronze_run_id == "bronze-run-1"
    assert result.new_count == 1
    assert result.duplicate_count == 0

    deps.marshal.register.assert_called_once()
    deps.marshal.record_bronze_artifacts.assert_called_once()

    registered_file = deps.marshal.register.call_args.args[0]

    assert registered_file.dataset_id == "fl_healthcharts.county"
    assert registered_file.source_file_name == "county.csv"
    assert registered_file.row_count_raw == 2
    assert registered_file.column_count_raw == 4

    quality_records = [
        call.args[0] for call in (deps.quality_checks.insert_quality_check_run.call_args_list)
    ]

    per_file = ["file_not_empty", "sha256_recorded", "xlsx_package_integrity", "xlsx_has_data_rows"]

    assert [record["check_id"].rsplit(".", 1)[-1] for record in quality_records] == [
        *per_file,
        *per_file,
        "output_files_created",
    ]

    data_row_records = [
        record for record in quality_records if record["check_id"].endswith(".xlsx_has_data_rows")
    ]

    assert [record["actual"]["row_count"] for record in data_row_records] == [1, 1]
    assert [record["actual"]["sheet_name"] for record in data_row_records] == [0, 0]
    assert [record["actual"]["header_row"] for record in data_row_records] == [0, 0]

    assert all(str(tmp_path) not in str(record["actual"]) for record in quality_records)

    execution_events = [
        call.args[0] for call in (deps.execution_logs.insert_execution_log.call_args_list)
    ]

    assert [event.event_type for event in execution_events] == ["step_started", "step_finished"]

    started_event, finished_event = execution_events

    assert started_event.step == "assemble_xlsx_batch"
    assert finished_event.step == "assemble_xlsx_batch"
    assert finished_event.status == "success"
    assert finished_event.counts.success == 1
    assert finished_event.counts.failed == 0

    assert finished_event.meta is not None
    assert finished_event.meta.dataset_id == "fl_healthcharts.county"
    assert finished_event.meta.dataset_file_id == "dataset-file-1"
    assert finished_event.meta.bronze_run_id == "bronze-run-1"
    assert finished_event.meta.input_file_count == 2
    assert finished_event.meta.output_file_count == 1
    assert finished_event.meta.output_row_count == 2
    assert finished_event.meta.output_column_count == 4
