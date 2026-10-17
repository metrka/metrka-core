from pathlib import Path

import pytest

from metrka_core.datasets.source_config import load_source_config
from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataColumn,
    FilenameMetadataConfig,
    build_filename_member_key,
    extract_filename_metadata,
    index_paths_by_filename_member_key,
)


def _config() -> FilenameMetadataConfig:
    return FilenameMetadataConfig(
        regex=(
            r"^cid(?P<cid_id>[0-9]+)__"
            r"(?P<year_breakdown>[^_]+)__"
            r"(?P<group_dimension>[^_]+)__"
            r"(?P<group_value>[^_]+)__"
            r"(?P<year>[0-9]{4})\.xlsx$"
        ),
        columns={
            "cid_id": FilenameMetadataColumn(from_group="cid_id", value_type="string"),
            "year_breakdown": FilenameMetadataColumn(
                from_group="year_breakdown", value_type="string"
            ),
            "group_dimension": FilenameMetadataColumn(
                from_group="group_dimension", value_type="string", null_values=("default",)
            ),
            "group_value": FilenameMetadataColumn(
                from_group="group_value", value_type="string", null_values=("all",)
            ),
            "reporting_year": FilenameMetadataColumn(from_group="year", value_type="integer"),
        },
        member_key=("cid_id", "year_breakdown", "group_dimension", "group_value", "reporting_year"),
    )


def test_extracts_default_filename_metadata() -> None:
    metadata = extract_filename_metadata("cid0314__single-year__default__all__2002.xlsx", _config())

    assert metadata == {
        "cid_id": "0314",
        "year_breakdown": "single-year",
        "group_dimension": None,
        "group_value": None,
        "reporting_year": 2002,
    }


def test_extracts_grouped_filename_metadata() -> None:
    metadata = extract_filename_metadata("cid0314__single-year__sex__female__2002.xlsx", _config())

    assert metadata == {
        "cid_id": "0314",
        "year_breakdown": "single-year",
        "group_dimension": "sex",
        "group_value": "female",
        "reporting_year": 2002,
    }


def test_rejects_column_using_unknown_regex_group() -> None:
    with pytest.raises(ValueError, match="unknown regex group"):
        FilenameMetadataConfig(
            regex=r"^cid(?P<cid_id>[0-9]+)\.xlsx$",
            columns={
                "reporting_year": FilenameMetadataColumn(from_group="year", value_type="integer")
            },
            member_key=("reporting_year",),
        )


def test_source_config_loads_filename_metadata(tmp_path: Path) -> None:
    config_path = tmp_path / "main.yaml"
    config_path.write_text(
        """
workspace_name: fl_healthcharts

streams:
  adult_substance_abuse_beds:
    official_filename: "cid0314__*.xlsx"

    filename_metadata:
      regex: '^cid(?P<cid_id>[0-9]+)__(?P<year_breakdown>[^_]+)__(?P<group_dimension>[^_]+)__(?P<group_value>[^_]+)__(?P<year>[0-9]{4})\\.xlsx$'

      columns:
        cid_id:
          from_group: cid_id
          type: string

        reporting_year:
          from_group: year
          type: integer

      member_key:
        - cid_id
        - reporting_year
""",
        encoding="utf-8",
    )

    source_config = load_source_config(config_path)
    filename_metadata = source_config.streams["adult_substance_abuse_beds"].filename_metadata

    assert filename_metadata is not None
    assert filename_metadata.member_key == ("cid_id", "reporting_year")
    assert extract_filename_metadata(
        "cid0314__single-year__default__all__2002.xlsx", filename_metadata
    ) == {"cid_id": "0314", "reporting_year": 2002}


def test_builds_member_key_in_configured_order() -> None:
    config = _config()
    metadata = extract_filename_metadata("cid0314__single-year__sex__female__2002.xlsx", config)

    member_key = build_filename_member_key(metadata, config)

    assert member_key == ("0314", "single-year", "sex", "female", 2002)


def test_rejects_duplicate_filename_member_keys(tmp_path: Path) -> None:
    filename = "cid0314__single-year__sex__female__2002.xlsx"

    first_path = tmp_path / "capture-1" / filename
    second_path = tmp_path / "capture-2" / filename

    with pytest.raises(ValueError, match="Duplicate filename member key"):
        index_paths_by_filename_member_key((first_path, second_path), _config())


def test_source_config_loads_xlsx_row_assembly(tmp_path: Path) -> None:
    config_path = tmp_path / "main.yaml"
    config_path.write_text(
        """
workspace_name: fl_healthcharts

streams:
  adult_substance_abuse_beds:
    official_filename: "cid0314__*.xlsx"

    bronze_assembly:
      strategy: xlsx_rows
      output_filename: adult_substance_abuse_beds.csv
      sheet_name: 0
      header_row: 2
      drop_fully_empty_rows: true
      drop_fully_empty_columns: true
""",
        encoding="utf-8",
    )

    source_config = load_source_config(config_path)
    assembly = source_config.streams["adult_substance_abuse_beds"].bronze_assembly

    assert assembly is not None
    assert assembly.strategy == "xlsx_rows"
    assert assembly.output_filename == "adult_substance_abuse_beds.csv"
    assert assembly.read_config.sheet_name == 0
    assert assembly.read_config.header_row == 2
    assert assembly.read_config.drop_fully_empty_rows is True
    assert assembly.read_config.drop_fully_empty_columns is True
