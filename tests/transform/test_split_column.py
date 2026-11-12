"""A contract column split at a separator into columns of their own."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from metrka_core.lineage.transformation.models import (
    AutomaticColumnEvidence,
    TransformationEvidenceKind,
)
from metrka_core.quality.config import contract_tables
from metrka_core.transform.schema import apply_transformation
from metrka_core.transform.validation import ContractValidationError, validate_contract_file


def _race_sex(**split: Any) -> dict[str, Any]:
    return {
        "rename_to": "race_sex",
        "cast_to": "string",
        "split": {
            "separator": "/",
            "into": {"race": {"cast_to": "string"}, "sex": {"cast_to": "string"}},
            **split,
        },
    }


def _write_contract(
    tmp_path: Path,
    race_sex: dict[str, Any],
    canonical_order: list[str] | None = None,
    **columns: dict[str, Any],
) -> Path:
    contract = {
        "tables": {
            "inmates": {
                "columns": {
                    "booking": {"rename_to": "booking_number", "cast_to": "string"},
                    "Race/Sex": race_sex,
                    **columns,
                },
                "canonical_order": canonical_order
                or ["booking_number", "race_sex", "race", "sex", *columns],
            }
        }
    }
    path = tmp_path / "inmates.yaml"
    path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
    return path


def test_a_column_splits_into_its_parts_and_is_still_published(tmp_path: Path) -> None:
    contract = validate_contract_file(_write_contract(tmp_path, _race_sex()))
    source = pd.DataFrame(
        {"booking": ["1", "2", "3", "4"], "Race/Sex": ["W/F", " B / M ", None, "W/"]},
        dtype="string",
    )

    result = apply_transformation(source, contract["tables"]["inmates"])

    assert list(result.data.columns) == ["booking_number", "race_sex", "race", "sex"]
    assert result.data["race_sex"].tolist()[:2] == ["W/F", " B / M "]
    assert result.data["race"].tolist() == ["W", "B", pd.NA, "W"]
    assert result.data["sex"].tolist() == ["F", "M", pd.NA, pd.NA]

    [split] = [
        item
        for item in result.evidence
        if isinstance(item, AutomaticColumnEvidence)
        and item.kind is TransformationEvidenceKind.COLUMN_SPLIT
    ]
    assert split.column_name == "race_sex"
    assert split.affected_row_count == 3
    assert split.reason == "Split race_sex at '/' into race, sex."
    assert split.metrics == {
        "separator": "/",
        "into": ["race", "sex"],
        "column_excluded": False,
        "split_value_count": 3,
        "missing_value_count": 1,
        "empty_part_counts": {"race": 0, "sex": 1},
    }


def test_exclude_column_publishes_only_the_parts(tmp_path: Path) -> None:
    path = _write_contract(
        tmp_path, _race_sex(exclude_column=True), canonical_order=["booking_number", "race", "sex"]
    )
    contract = validate_contract_file(path)
    source = pd.DataFrame({"booking": ["1"], "Race/Sex": ["W/F"]}, dtype="string")

    result = apply_transformation(source, contract["tables"]["inmates"])

    assert list(result.data.columns) == ["booking_number", "race", "sex"]
    assert result.data.loc[0, "race"] == "W"
    [split] = [
        item
        for item in result.evidence
        if isinstance(item, AutomaticColumnEvidence)
        and item.kind is TransformationEvidenceKind.COLUMN_SPLIT
    ]
    assert split.reason == "Split race_sex at '/' into race, sex; race_sex itself is not published."
    assert split.metrics["column_excluded"] is True
    assert contract_tables(contract)["inmates"] == {
        "booking_number": "string",
        "race": "string",
        "sex": "string",
    }


def test_each_part_is_cast_by_its_own_rule(tmp_path: Path) -> None:
    released = {
        "rename_to": "release",
        "cast_to": "string",
        "split": {
            "separator": " | ",
            "into": {
                "release_date": {"cast_to": "date", "format_in": "%m/%d/%Y"},
                "release_days": {"cast_to": "int"},
                "release_reason": {"cast_to": "string", "case": "lower"},
            },
        },
    }
    path = _write_contract(
        tmp_path,
        _race_sex(),
        canonical_order=[
            "booking_number",
            "race_sex",
            "race",
            "sex",
            "release",
            "release_date",
            "release_days",
            "release_reason",
        ],
        Release=released,
    )
    contract = validate_contract_file(path)
    source = pd.DataFrame(
        {"booking": ["1"], "Race/Sex": ["W/F"], "Release": ["01/02/2026 | 30 | BOND"]},
        dtype="string",
    )

    data = apply_transformation(source, contract["tables"]["inmates"]).data

    assert data["release_date"].tolist() == [pd.Timestamp("2026-01-02").date()]
    assert str(data["release_days"].dtype) == "Int64"
    assert data["release_days"].tolist() == [30]
    assert data["release_reason"].tolist() == ["bond"]


def test_a_value_with_the_wrong_number_of_parts_stops_the_table(tmp_path: Path) -> None:
    contract = validate_contract_file(_write_contract(tmp_path, _race_sex()))
    source = pd.DataFrame(
        {"booking": ["1", "2", "3", "4"], "Race/Sex": ["W/F", "W", "W/F/X", "W"]}, dtype="string"
    )

    with pytest.raises(
        ValueError,
        match=r"3 value\(s\) of race_sex do not split at '/' into 2 parts \(race, sex\), "
        r"such as \['W', 'W/F/X'\]",
    ):
        apply_transformation(source, contract["tables"]["inmates"])


def test_quality_rules_see_the_parts_with_their_types(tmp_path: Path) -> None:
    contract = validate_contract_file(_write_contract(tmp_path, _race_sex()))

    assert contract_tables(contract)["inmates"] == {
        "booking_number": "string",
        "race_sex": "string",
        "race": "string",
        "sex": "string",
    }


@pytest.mark.parametrize(
    ("race_sex", "canonical_order", "message"),
    [
        (
            _race_sex(),
            ["booking_number", "race_sex", "race"],
            "missing renamed columns: \\['sex'\\]",
        ),
        (
            _race_sex(),
            ["booking_number", "race", "sex"],
            "missing renamed columns: \\['race_sex'\\]",
        ),
        (_race_sex(separator=""), None, "separator must be a non-empty string"),
        (
            _race_sex(into={"race": {"cast_to": "string"}}),
            ["booking_number", "race_sex", "race"],
            "at least two column names",
        ),
        (
            _race_sex(into={"race": {"cast_to": "string"}, "sex": {}}),
            None,
            "into.sex: missing or invalid 'cast_to'",
        ),
        (
            _race_sex(into={"race": {"cast_to": "string"}, "sex": {"cast_to": "date"}}),
            None,
            "into.sex: 'cast_to' requires a non-empty string 'format_in' rule",
        ),
        (
            _race_sex(
                into={"race": {"cast_to": "string"}, "sex": {"cast_to": "string", "rename_to": "x"}}
            ),
            None,
            "into.sex: unsupported rule keys: \\['rename_to'\\]",
        ),
        (
            _race_sex(into={"race_sex": {"cast_to": "string"}, "sex": {"cast_to": "string"}}),
            ["booking_number", "race_sex", "sex"],
            "duplicate renamed columns found: \\['race_sex'\\]",
        ),
        ({**_race_sex(), "cast_to": "int"}, None, "a split column must use cast_to: string"),
        (
            {**_race_sex(), "split": {"separator": "/"}},
            None,
            "must be a mapping with separator, into and optionally exclude_column",
        ),
        (
            _race_sex(exclude_column="yes"),
            ["booking_number", "race", "sex"],
            "exclude_column must be true or false",
        ),
        (
            _race_sex(exclude_column=True),
            None,
            "canonical_order contains unknown columns: \\['race_sex'\\]",
        ),
        (
            _race_sex(drop=True),
            None,
            "must be a mapping with separator, into and optionally exclude_column",
        ),
    ],
)
def test_contract_validation_refuses_a_broken_split(
    tmp_path: Path, race_sex: dict[str, Any], canonical_order: list[str] | None, message: str
) -> None:
    with pytest.raises(ContractValidationError, match=message):
        validate_contract_file(_write_contract(tmp_path, race_sex, canonical_order))
