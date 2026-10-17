from pathlib import Path

import pandas as pd
import pytest

from metrka_core.pipeline.bronze.filename_metadata import (
    FilenameMetadataColumn,
    FilenameMetadataConfig,
)
from metrka_core.pipeline.bronze.xlsx_row_assembly import (
    XlsxReadConfig,
    assemble_xlsx_rows,
    write_assembled_xlsx_csv,
)


def _filename_config() -> FilenameMetadataConfig:
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


def test_assembles_xlsx_rows_with_filename_metadata(tmp_path: Path) -> None:
    file_2002 = tmp_path / "cid0314__single-year__default__all__2002.xlsx"
    file_2003 = tmp_path / "cid0314__single-year__sex__female__2003.xlsx"

    pd.DataFrame([{"County": "Florida", "Count": 10}]).to_excel(file_2002, index=False)

    pd.DataFrame([{"County": "Florida", "Count": 12}]).to_excel(file_2003, index=False)

    result = assemble_xlsx_rows(
        paths=(file_2003, file_2002),
        filename_config=_filename_config(),
        read_config=XlsxReadConfig(),
    )

    assert result.to_dict(orient="records") == [
        {
            "County": "Florida",
            "Count": 10,
            "cid_id": "0314",
            "year_breakdown": "single-year",
            "group_dimension": None,
            "group_value": None,
            "reporting_year": 2002,
        },
        {
            "County": "Florida",
            "Count": 12,
            "cid_id": "0314",
            "year_breakdown": "single-year",
            "group_dimension": "sex",
            "group_value": "female",
            "reporting_year": 2003,
        },
    ]


def test_writes_one_combined_bronze_csv(tmp_path: Path) -> None:
    file_2002 = tmp_path / "cid0314__single-year__default__all__2002.xlsx"
    file_2003 = tmp_path / "cid0314__single-year__sex__female__2003.xlsx"

    pd.DataFrame([{"County": "Florida", "Count": 10}]).to_excel(file_2002, index=False)

    pd.DataFrame([{"County": "Florida", "Count": 12}]).to_excel(file_2003, index=False)

    output_path = tmp_path / "bronze" / "adult_substance_abuse_beds.csv"

    result = write_assembled_xlsx_csv(
        paths=(file_2003, file_2002),
        output_path=output_path,
        filename_config=_filename_config(),
        read_config=XlsxReadConfig(),
    )

    written = pd.read_csv(output_path)

    assert result.output_path == output_path
    assert result.input_file_count == 2
    assert result.row_count == 2
    assert result.column_count == 7

    assert written["reporting_year"].tolist() == [2002, 2003]
    assert written["Count"].tolist() == [10, 12]


def test_rejects_mismatched_xlsx_source_columns(tmp_path: Path) -> None:
    file_2002 = tmp_path / "cid0314__single-year__default__all__2002.xlsx"
    file_2003 = tmp_path / "cid0314__single-year__default__all__2003.xlsx"

    pd.DataFrame([{"County": "Florida", "Count": 10}]).to_excel(file_2002, index=False)

    pd.DataFrame([{"County": "Florida", "Rate": 1.2}]).to_excel(file_2003, index=False)

    with pytest.raises(ValueError) as error:
        assemble_xlsx_rows(
            paths=(file_2002, file_2003),
            filename_config=_filename_config(),
            read_config=XlsxReadConfig(),
        )

    message = str(error.value)

    assert "XLSX source columns do not match" in message
    assert file_2002.name in message
    assert file_2003.name in message
    assert "missing=['Count']" in message
    assert "extra=['Rate']" in message


def test_rejects_reordered_xlsx_source_columns(tmp_path: Path) -> None:
    file_2002 = tmp_path / "cid0314__single-year__default__all__2002.xlsx"
    file_2003 = tmp_path / "cid0314__single-year__default__all__2003.xlsx"

    pd.DataFrame([{"County": "Florida", "Count": 10}], columns=["County", "Count"]).to_excel(
        file_2002, index=False
    )

    pd.DataFrame([{"Count": 12, "County": "Florida"}], columns=["Count", "County"]).to_excel(
        file_2003, index=False
    )

    with pytest.raises(ValueError) as error:
        assemble_xlsx_rows(
            paths=(file_2002, file_2003),
            filename_config=_filename_config(),
            read_config=XlsxReadConfig(),
        )

    message = str(error.value)

    assert "XLSX source columns do not match" in message
    assert file_2002.name in message
    assert file_2003.name in message
    assert "missing=[]" in message
    assert "extra=[]" in message
    assert "reordered=True" in message


def test_does_not_write_csv_when_xlsx_columns_do_not_match(tmp_path: Path) -> None:
    file_2002 = tmp_path / "cid0314__single-year__default__all__2002.xlsx"
    file_2003 = tmp_path / "cid0314__single-year__default__all__2003.xlsx"
    output_path = tmp_path / "bronze" / "combined.csv"

    pd.DataFrame([{"County": "Florida", "Count": 10}]).to_excel(file_2002, index=False)

    pd.DataFrame([{"County": "Florida", "Rate": 1.2}]).to_excel(file_2003, index=False)

    with pytest.raises(ValueError, match="XLSX source columns do not match"):
        write_assembled_xlsx_csv(
            paths=(file_2002, file_2003),
            output_path=output_path,
            filename_config=_filename_config(),
            read_config=XlsxReadConfig(),
        )

    assert not output_path.exists()
