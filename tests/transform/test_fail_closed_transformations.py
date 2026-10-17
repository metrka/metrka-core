from __future__ import annotations

import pandas as pd
import pytest

from metrka_core.transform.ops.casting import cast_columns
from metrka_core.transform.ops.dates import parse_dates
from metrka_core.transform.ops.text import convert_case, normalize_values
from metrka_core.transform.schema import apply_transformation


def test_schema_transformation_rejects_missing_source_columns() -> None:
    source = pd.DataFrame({"actual": ["value"]})
    config = {"columns": {"required": {"cast_to": "string"}}}

    with pytest.raises(KeyError, match="Missing expected source columns"):
        apply_transformation(source, config)


def test_value_normalization_rejects_missing_target_column() -> None:
    source = pd.DataFrame({"actual": ["value"]})
    rules = {
        "required": {"old": {"replace_with": "new", "reason": "Correct a known source value."}}
    }

    with pytest.raises(KeyError, match="normalize_values column not found"):
        normalize_values(source, rules)


def test_value_normalization_rejects_missing_detail_columns() -> None:
    source = pd.DataFrame({"status": ["old"]})
    rules = {
        "status": {
            "old": {
                "replace_with": "new",
                "reason": "Correct a known source value.",
                "record_details": True,
                "detail_columns": ["source_record_id"],
            }
        }
    }

    with pytest.raises(KeyError, match="normalize_values detail columns not found"):
        normalize_values(source, rules)


def test_cast_rejects_missing_target_column() -> None:
    source = pd.DataFrame({"actual": ["1"]})

    with pytest.raises(KeyError, match="cast column not found"):
        cast_columns(source, {"required": "int"})


def test_cast_rejects_unknown_target_type() -> None:
    source = pd.DataFrame({"value": ["1"]})

    with pytest.raises(ValueError, match="Unknown cast"):
        cast_columns(source, {"value": "mystery"})


def test_date_parse_rejects_missing_target_column() -> None:
    source = pd.DataFrame({"actual": ["08/16/2026"]})

    with pytest.raises(KeyError, match="Date column not found"):
        parse_dates(source, {"required": {"cast_to": "date", "format_in": "%m/%d/%Y"}})


def test_case_conversion_rejects_missing_target_column() -> None:
    source = pd.DataFrame({"actual": ["value"]})

    with pytest.raises(KeyError, match="Column not found"):
        convert_case(source, {"required": "upper"})


def test_case_conversion_rejects_unknown_mode() -> None:
    source = pd.DataFrame({"value": ["text"]})

    with pytest.raises(ValueError, match="Unknown case rule"):
        convert_case(source, {"value": "sideways"})
