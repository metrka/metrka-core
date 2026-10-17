from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa  # type: ignore[import-untyped]
import pytest
import yaml

from metrka_core.lineage.transformation.models import AutomaticColumnEvidence
from metrka_core.transform.cast_types import (
    DATE_LIKE_INPUT_TYPES,
    SilverCastType,
    resolve_silver_cast_type,
)
from metrka_core.transform.ops.dates import parse_dates
from metrka_core.transform.schema import apply_transformation
from metrka_core.transform.validation import ContractValidationError, validate_contract_file


def _write_contract(tmp_path: Path, *, cast_type: SilverCastType) -> Path:
    rule = {"rename_to": "value", "cast_to": cast_type.value}

    if cast_type in DATE_LIKE_INPUT_TYPES:
        rule["format_in"] = "%m/%d/%Y"

    contract_path = tmp_path / f"{cast_type.value}.yaml"
    contract_path.write_text(
        yaml.safe_dump(
            {"tables": {"example": {"columns": {"source": rule}, "canonical_order": ["value"]}}},
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return contract_path


@pytest.mark.parametrize("cast_type", list(SilverCastType))
def test_validator_accepts_every_declared_simple_cast_type(
    tmp_path: Path, cast_type: SilverCastType
) -> None:
    contract_path = _write_contract(tmp_path, cast_type=cast_type)

    validated = validate_contract_file(contract_path)

    assert validated["tables"]["example"]["columns"]["source"]["cast_to"] == cast_type.value


def test_timestamp_is_a_public_alias_for_datetime() -> None:
    assert resolve_silver_cast_type("timestamp") is SilverCastType.DATETIME


def test_validator_rejects_non_string_date_format(tmp_path: Path) -> None:
    contract_path = tmp_path / "invalid-date-format.yaml"
    contract_path.write_text(
        yaml.safe_dump(
            {
                "tables": {
                    "example": {
                        "columns": {
                            "source": {
                                "rename_to": "value",
                                "cast_to": "date",
                                "format_in": 20260816,
                            }
                        },
                        "canonical_order": ["value"],
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ContractValidationError, match="non-empty string 'format_in'"):
        validate_contract_file(contract_path)


@pytest.mark.parametrize(
    ("declared_type", "canonical_type"),
    [
        (SilverCastType.DATE, SilverCastType.DATE),
        (SilverCastType.DATETIME, SilverCastType.DATETIME),
        (SilverCastType.TIMESTAMP, SilverCastType.DATETIME),
    ],
)
def test_date_like_types_use_the_date_parser(
    declared_type: SilverCastType, canonical_type: SilverCastType
) -> None:
    source = pd.DataFrame({"source": pd.Series(["08/16/2026"], dtype="string")})
    config = {
        "columns": {
            "source": {
                "rename_to": "value",
                "cast_to": declared_type.value,
                "format_in": "%m/%d/%Y",
            }
        },
        "canonical_order": ["value"],
    }

    result = apply_transformation(source, config)

    date_evidence = [
        evidence
        for evidence in result.evidence
        if isinstance(evidence, AutomaticColumnEvidence) and evidence.operation == "parse_dates"
    ]

    assert len(date_evidence) == 1
    assert date_evidence[0].metrics["target_type"] == canonical_type.value

    if canonical_type is SilverCastType.DATE:
        assert isinstance(result.data["value"].dtype, pd.ArrowDtype)
        assert result.data["value"].dtype.pyarrow_dtype == pa.date32()
    else:
        assert pd.api.types.is_datetime64_any_dtype(result.data["value"].dtype)


def test_date_parser_rejects_a_non_date_cast_type() -> None:
    source = pd.DataFrame({"value": pd.Series(["08/16/2026"], dtype="string")})

    with pytest.raises(ValueError, match="non-date cast type 'string'"):
        parse_dates(source, {"value": {"cast_to": "string", "format_in": "%m/%d/%Y"}})
