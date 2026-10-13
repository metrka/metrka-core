from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataColumn,
    FilenameMetadataConfig,
)
from metrka_core.pipeline.bronze.xlsx_batch_ingestion import build_xlsx_batch_marshaled_file
from metrka_core.pipeline.bronze.xlsx_batch_preparation import prepare_xlsx_source_batch
from metrka_core.pipeline.bronze.xlsx_row_assembly import BronzeAssemblyConfig, XlsxReadConfig
from metrka_core.pipeline.models import LandedAsset


def test_prepares_one_bronze_batch_from_multiple_xlsx_files(tmp_path: Path) -> None:
    first = tmp_path / "cid0321__single-year__default__all__2002.xlsx"
    second = tmp_path / "cid0321__single-year__sex__female__2025.xlsx"

    pd.DataFrame([{"County": "Florida", "Count": 10}]).to_excel(first, index=False)

    pd.DataFrame([{"County": "Florida", "Count": 12}]).to_excel(second, index=False)

    assets = (
        LandedAsset(
            stream_name="county",
            path=first,
            source_url="manual_upload",
            source_capture_id="capture-1",
        ),
        LandedAsset(
            stream_name="county",
            path=second,
            source_url="manual_upload",
            source_capture_id="capture-1",
        ),
    )

    filename_config = FilenameMetadataConfig(
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

    assembly_config = BronzeAssemblyConfig(
        strategy="xlsx_rows", output_filename="county.csv", read_config=XlsxReadConfig()
    )

    result = prepare_xlsx_source_batch(
        assets=assets,
        output_dir=tmp_path / "bronze-run",
        filename_config=filename_config,
        assembly_config=assembly_config,
    )

    assert result.stream_name == "county"
    assert result.source_capture_ids == ("capture-1",)
    assert result.source_urls == ("manual_upload",)
    assert result.input_file_count == 2
    assert result.row_count == 2
    assert result.column_count == 4
    assert result.output_path == tmp_path / "bronze-run" / "county.csv"
    assert result.output_path.is_file()

    assert tuple(member.filename for member in result.fingerprint.members) == (
        "cid0321__single-year__default__all__2002.xlsx",
        "cid0321__single-year__sex__female__2025.xlsx",
    )

    ingested_at = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)

    marshaled_file = build_xlsx_batch_marshaled_file(
        prepared=result,
        dataset_id="fl_healthcharts.county",
        dataset_file_id="dataset-file-1",
        ingested_at=ingested_at,
    )

    assert marshaled_file.dataset_file_id == "dataset-file-1"
    assert marshaled_file.dataset_id == "fl_healthcharts.county"
    assert marshaled_file.source_url == "manual_upload"
    assert marshaled_file.source_file_name == "county.csv"
    assert marshaled_file.original_source_file_name == "county.csv"
    assert marshaled_file.source_hash == result.fingerprint.sha256
    assert marshaled_file.file_size == sum(
        member.size_bytes for member in result.fingerprint.members
    )
    assert marshaled_file.ingestion_timestamp == ingested_at
    assert marshaled_file.row_count_raw == 2
    assert marshaled_file.column_count_raw == 4
    assert marshaled_file.artifact_role == "data"
