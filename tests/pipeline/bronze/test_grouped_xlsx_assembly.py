"""Bronze assembly that sorts whole XLSX files into one CSV per filename metadata value."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from metrka_core.datasets.source_config import load_source_config
from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataColumn,
    FilenameMetadataConfig,
)
from metrka_core.pipeline.bronze.xlsx_batch_ingestion import build_xlsx_batch_marshaled_file
from metrka_core.pipeline.bronze.xlsx_batch_preparation import prepare_xlsx_source_batch
from metrka_core.pipeline.bronze.xlsx_row_assembly import (
    BronzeAssemblyConfig,
    XlsxReadConfig,
    split_skipped_paths,
    write_assembled_xlsx_csvs,
)
from metrka_core.pipeline.models import LandedAsset

OUTPUTS = (
    ("crude", "lung_cancer_incidence.csv"),
    ("age_adjusted", "lung_cancer_incidence_age_adjusted.csv"),
)


def _filename_config() -> FilenameMetadataConfig:
    return FilenameMetadataConfig(
        regex=(
            r"^cid(?P<cid_id>[0-9]+)__(?P<year_breakdown>[^_]+)__"
            r"(?P<rate_type>crude|age_adjusted|other)__(?P<group>[a-z]+)__"
            r"(?P<year>[0-9]{4})\.xlsx$"
        ),
        columns={
            "reporting_year": FilenameMetadataColumn(from_group="year", value_type="integer"),
            "population_group": FilenameMetadataColumn(from_group="group", value_type="string"),
            "rate_type": FilenameMetadataColumn(from_group="rate_type", value_type="string"),
        },
        member_key=("rate_type", "population_group", "reporting_year"),
    )


def _grouped_config() -> BronzeAssemblyConfig:
    return BronzeAssemblyConfig(
        strategy="xlsx_rows",
        output_filename=None,
        read_config=XlsxReadConfig(),
        group_files_by="rate_type",
        outputs=OUTPUTS,
    )


def _write_crude(path: Path, count: int) -> Path:
    rows = [{"County": "Florida", "Count": count, "Denom": 1000, "Rate": 1.5, "MOV": 0.2}]
    pd.DataFrame(rows).to_excel(path, index=False)
    return path


def _write_age_adjusted(path: Path, count: int) -> Path:
    pd.DataFrame([{"County": "Florida", "Count": count, "Rate": 1.2}]).to_excel(path, index=False)
    return path


def _lung_files(folder: Path) -> list[Path]:
    folder.mkdir(parents=True, exist_ok=True)
    return [
        _write_crude(folder / "cid0446__single-year__crude__white__2023.xlsx", 10),
        _write_crude(folder / "cid0446__single-year__crude__allraceethnicity__2023.xlsx", 30),
        _write_age_adjusted(folder / "cid0446__single-year__age_adjusted__white__2023.xlsx", 10),
        _write_age_adjusted(
            folder / "cid0446__single-year__age_adjusted__allraceethnicity__2023.xlsx", 30
        ),
    ]


def test_grouped_assembly_writes_one_csv_per_value(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")

    results = write_assembled_xlsx_csvs(
        paths=files,
        output_dir=tmp_path / "bronze",
        filename_config=_filename_config(),
        assembly_config=_grouped_config(),
    )

    assert [result.output_path.name for result in results] == [
        "lung_cancer_incidence.csv",
        "lung_cancer_incidence_age_adjusted.csv",
    ]
    crude = pd.read_csv(tmp_path / "bronze" / "lung_cancer_incidence.csv")
    adjusted = pd.read_csv(tmp_path / "bronze" / "lung_cancer_incidence_age_adjusted.csv")

    # Each file goes whole into the output of its rate type, with its own columns.
    assert list(crude.columns) == [
        "County",
        "Count",
        "Denom",
        "Rate",
        "MOV",
        "reporting_year",
        "population_group",
        "rate_type",
    ]
    assert list(adjusted.columns) == [
        "County",
        "Count",
        "Rate",
        "reporting_year",
        "population_group",
        "rate_type",
    ]
    assert set(crude["rate_type"]) == {"crude"}
    assert set(adjusted["rate_type"]) == {"age_adjusted"}
    assert sorted(crude["population_group"]) == ["allraceethnicity", "white"]
    assert [result.input_file_count for result in results] == [2, 2]
    assert [result.row_count for result in results] == [2, 2]


def test_a_value_without_an_output_is_an_error(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")
    files.append(
        _write_crude(tmp_path / "landing" / "cid0446__single-year__other__white__2023.xlsx", 1)
    )

    with pytest.raises(ValueError, match="rate_type values that no output names: 'other'"):
        write_assembled_xlsx_csvs(
            paths=files,
            output_dir=tmp_path / "bronze",
            filename_config=_filename_config(),
            assembly_config=_grouped_config(),
        )

    assert not (tmp_path / "bronze").exists()


def test_an_output_without_files_is_an_error(tmp_path: Path) -> None:
    files = [path for path in _lung_files(tmp_path / "landing") if "__crude__" in path.name]

    with pytest.raises(ValueError, match=r"No files have rate_type value\(s\) \['age_adjusted'\]"):
        write_assembled_xlsx_csvs(
            paths=files,
            output_dir=tmp_path / "bronze",
            filename_config=_filename_config(),
            assembly_config=_grouped_config(),
        )


def test_mismatched_columns_within_a_group_write_no_output(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")
    # A crude file shaped like an age-adjusted one: its group no longer has one column set.
    files.append(
        _write_age_adjusted(
            tmp_path / "landing" / "cid0446__single-year__crude__black__2023.xlsx", 5
        )
    )

    with pytest.raises(ValueError, match="XLSX source columns do not match"):
        write_assembled_xlsx_csvs(
            paths=files,
            output_dir=tmp_path / "bronze",
            filename_config=_filename_config(),
            assembly_config=_grouped_config(),
        )

    assert not (tmp_path / "bronze").exists()


def test_prepared_batch_lists_every_output(tmp_path: Path) -> None:
    assets = tuple(
        LandedAsset(
            stream_name="lung_cancer_incidence",
            path=path,
            source_url="https://example.test/indicator",
            source_capture_id="capture-1",
        )
        for path in _lung_files(tmp_path / "landing")
    )

    prepared = prepare_xlsx_source_batch(
        assets=assets,
        output_dir=tmp_path / "bronze",
        filename_config=_filename_config(),
        assembly_config=_grouped_config(),
    )

    assert [path.name for path in prepared.output_paths] == [
        "lung_cancer_incidence.csv",
        "lung_cancer_incidence_age_adjusted.csv",
    ]
    assert prepared.input_file_count == 4
    assert prepared.row_count == 4
    assert prepared.column_count == 8

    marshaled = build_xlsx_batch_marshaled_file(
        prepared=prepared,
        dataset_id="fl_healthcharts.lung_cancer_incidence",
        dataset_file_id="dataset-file-1",
        ingested_at=pd.Timestamp("2026-10-17T12:00:00Z").to_pydatetime(),
    )

    assert marshaled.source_file_name == (
        "lung_cancer_incidence.csv, lung_cancer_incidence_age_adjusted.csv"
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"output_filename": "a.csv", "group_files_by": "rate_type"}, "not both"),
        ({"output_filename": None, "group_files_by": "rate_type"}, "needs outputs"),
        ({"output_filename": "a.csv", "outputs": OUTPUTS}, "outputs need group_files_by"),
        (
            {"output_filename": None, "group_files_by": "rate_type", "outputs": (("a", "x.txt"),)},
            "must end with .csv",
        ),
        (
            {
                "output_filename": None,
                "group_files_by": "rate_type",
                "outputs": (("a", "x.csv"), ("b", "X.csv")),
            },
            "repeat an output filename",
        ),
    ],
)
def test_assembly_config_rejects_unclear_outputs(kwargs: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        BronzeAssemblyConfig(strategy="xlsx_rows", read_config=XlsxReadConfig(), **kwargs)


def _main_yaml(bronze_assembly: str) -> str:
    return f"""
workspace_name: fl_healthcharts

streams:
  lung_cancer_incidence:
    official_filename: "cid0446__*.xlsx"
    filename_metadata:
      regex: '^cid(?P<cid_id>[0-9]+)__(?P<year_breakdown>[^_]+)__(?P<rate_type>[a-z_]+?)__(?P<group>[a-z]+)__(?P<year>[0-9]{{4}})[.]xlsx$'
      columns:
        reporting_year: {{from_group: year, type: integer}}
        rate_type: {{from_group: rate_type}}
      member_key: [rate_type, reporting_year]
    bronze_assembly:
{bronze_assembly}
"""


def test_source_config_loads_grouped_outputs(tmp_path: Path) -> None:
    config_path = tmp_path / "main.yaml"
    config_path.write_text(
        _main_yaml(
            """      strategy: xlsx_rows
      group_files_by: rate_type
      outputs:
        crude: lung_cancer_incidence.csv
        age_adjusted: lung_cancer_incidence_age_adjusted.csv"""
        ),
        encoding="utf-8",
    )

    assembly = load_source_config(config_path).streams["lung_cancer_incidence"].bronze_assembly

    assert assembly is not None
    assert assembly.output_filename is None
    assert assembly.group_files_by == "rate_type"
    assert assembly.outputs == OUTPUTS
    assert assembly.output_filenames == (
        "lung_cancer_incidence.csv",
        "lung_cancer_incidence_age_adjusted.csv",
    )


@pytest.mark.parametrize(
    ("assembly", "message"),
    [
        (
            """      strategy: xlsx_rows
      group_files_by: sex
      outputs: {female: f.csv}""",
            "group_files_by 'sex' is not a filename_metadata column",
        ),
        (
            """      strategy: xlsx_rows
      group_files_by: rate_type
      output_filename: one.csv
      outputs: {crude: c.csv}""",
            "output_filename or group_files_by, not both",
        ),
        (
            """      strategy: xlsx_rows
      group_files_by: rate_type""",
            "group_files_by needs outputs",
        ),
    ],
)
def test_source_config_rejects_unclear_grouping(
    tmp_path: Path, assembly: str, message: str
) -> None:
    config_path = tmp_path / "main.yaml"
    config_path.write_text(_main_yaml(assembly), encoding="utf-8")

    with pytest.raises(RuntimeError, match=message):
        load_source_config(config_path)


def _crude_only_config() -> BronzeAssemblyConfig:
    return BronzeAssemblyConfig(
        strategy="xlsx_rows",
        output_filename=None,
        read_config=XlsxReadConfig(),
        group_files_by="rate_type",
        outputs=(("crude", "lung_cancer_incidence.csv"),),
        skip_values=("age_adjusted",),
    )


def test_a_skipped_value_writes_no_output(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")

    results = write_assembled_xlsx_csvs(
        paths=files,
        output_dir=tmp_path / "bronze",
        filename_config=_filename_config(),
        assembly_config=_crude_only_config(),
    )

    assert [result.output_path.name for result in results] == ["lung_cancer_incidence.csv"]
    assert sorted(path.name for path in (tmp_path / "bronze").iterdir()) == [
        "lung_cancer_incidence.csv"
    ]
    assert set(pd.read_csv(tmp_path / "bronze" / "lung_cancer_incidence.csv")["rate_type"]) == {
        "crude"
    }


def test_split_skipped_paths_leaves_out_only_skipped_files(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")

    used, skipped = split_skipped_paths(
        paths=files, filename_config=_filename_config(), assembly_config=_crude_only_config()
    )

    assert sorted(path.name for path in used) == [
        "cid0446__single-year__crude__allraceethnicity__2023.xlsx",
        "cid0446__single-year__crude__white__2023.xlsx",
    ]
    assert sorted(path.name for path in skipped) == [
        "cid0446__single-year__age_adjusted__allraceethnicity__2023.xlsx",
        "cid0446__single-year__age_adjusted__white__2023.xlsx",
    ]


def test_split_skipped_paths_keeps_everything_without_skip_values(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")

    used, skipped = split_skipped_paths(
        paths=files, filename_config=_filename_config(), assembly_config=_grouped_config()
    )

    assert used == tuple(files)
    assert skipped == ()


def test_a_value_neither_output_nor_skipped_is_still_an_error(tmp_path: Path) -> None:
    files = _lung_files(tmp_path / "landing")
    files.append(
        _write_crude(tmp_path / "landing" / "cid0446__single-year__other__white__2023.xlsx", 1)
    )

    with pytest.raises(ValueError, match="skipped values: \\['age_adjusted'\\]"):
        write_assembled_xlsx_csvs(
            paths=files,
            output_dir=tmp_path / "bronze",
            filename_config=_filename_config(),
            assembly_config=_crude_only_config(),
        )


def test_a_skipped_value_without_files_is_fine(tmp_path: Path) -> None:
    files = [path for path in _lung_files(tmp_path / "landing") if "__crude__" in path.name]

    results = write_assembled_xlsx_csvs(
        paths=files,
        output_dir=tmp_path / "bronze",
        filename_config=_filename_config(),
        assembly_config=_crude_only_config(),
    )

    assert [result.input_file_count for result in results] == [2]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"output_filename": "a.csv", "skip_values": ("age_adjusted",)},
            "outputs need group_files_by",
        ),
        (
            {
                "output_filename": None,
                "group_files_by": "rate_type",
                "outputs": (("crude", "a.csv"),),
                "skip_values": ("crude",),
            },
            "both name \\['crude'\\]",
        ),
        (
            {
                "output_filename": None,
                "group_files_by": "rate_type",
                "outputs": (("crude", "a.csv"),),
                "skip_values": ("age_adjusted", "age_adjusted"),
            },
            "skip_values repeat",
        ),
    ],
)
def test_assembly_config_rejects_unclear_skips(kwargs: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        BronzeAssemblyConfig(strategy="xlsx_rows", read_config=XlsxReadConfig(), **kwargs)


def test_source_config_reads_skip_as_a_skipped_value(tmp_path: Path) -> None:
    config_path = tmp_path / "main.yaml"
    config_path.write_text(
        _main_yaml(
            """      strategy: xlsx_rows
      group_files_by: rate_type
      outputs:
        crude: lung_cancer_incidence.csv
        age_adjusted: skip"""
        ),
        encoding="utf-8",
    )

    assembly = load_source_config(config_path).streams["lung_cancer_incidence"].bronze_assembly

    assert assembly is not None
    assert assembly.outputs == (("crude", "lung_cancer_incidence.csv"),)
    assert assembly.skip_values == ("age_adjusted",)
    assert assembly.output_filenames == ("lung_cancer_incidence.csv",)


def test_source_config_needs_one_output_that_is_not_skipped(tmp_path: Path) -> None:
    config_path = tmp_path / "main.yaml"
    config_path.write_text(
        _main_yaml(
            """      strategy: xlsx_rows
      group_files_by: rate_type
      outputs: {crude: skip, age_adjusted: skip}"""
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="every value is 'skip'"):
        load_source_config(config_path)
