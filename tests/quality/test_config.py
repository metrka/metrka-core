"""quality.yaml parsing and validation against the Silver contract."""

from __future__ import annotations

from typing import Any

import pytest

from metrka_core.pipeline.silver.config_fingerprints import calculate_quality_config_hash
from metrka_core.quality.config import ColumnRule, parse_quality_config, validate_quality_config
from metrka_core.quality.models import QualitySeverity

CONTRACT: dict[str, Any] = {
    "tables": {
        "beds": {
            "columns": {
                "County": {"rename_to": "geography_name", "cast_to": "string"},
                "Count": {"rename_to": "licensed_bed_count", "cast_to": "int"},
                "Rate": {"rename_to": "rate", "cast_to": "decimal(12,1)"},
            }
        }
    }
}


def _config(tables: dict[str, Any]) -> Any:
    return parse_quality_config({"version": 1, "tables": tables})


def test_minimal_config_has_no_rules() -> None:
    config = parse_quality_config({"version": 1})

    assert config.tables == {}
    assert config.rule_count == 0


def test_rules_are_parsed_with_default_and_explicit_severity() -> None:
    config = _config(
        {
            "beds": {
                "unique": ["geography_name"],
                "columns": {
                    "licensed_bed_count": ["not_null", {"min": 0}],
                    "rate": [{"between": [0, 100], "severity": "warning"}],
                },
            }
        }
    )

    beds = config.tables["beds"]
    assert beds.unique == ("geography_name",)
    assert beds.columns["licensed_bed_count"] == (ColumnRule("not_null"), ColumnRule("min", 0))
    assert beds.columns["rate"] == (ColumnRule("between", (0, 100), QualitySeverity.WARNING),)
    assert config.rule_count == 4


@pytest.mark.parametrize(
    ("rules", "message"),
    [
        (["positive"], "Unknown rule"),
        ([{"minimum": 0}], "Unknown rule"),
        ([{"min": "zero"}], "needs a number"),
        ([{"min": True}], "needs a number"),
        ([{"between": [5, 1]}], "low <= high"),
        ([{"between": [1]}], "needs \\[low, high\\]"),
        ([{"allowed": []}], "non-empty list"),
        ([{"min": 0, "max": 1}], "exactly one rule"),
        ([{"min": 0, "severity": "fatal"}], "Unknown severity"),
    ],
)
def test_invalid_rules_are_rejected(rules: list[Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _config({"beds": {"columns": {"licensed_bed_count": rules}}})


def test_the_old_gates_format_and_unknown_versions_are_rejected() -> None:
    with pytest.raises(ValueError, match="Invalid quality config"):
        parse_quality_config({"version": 1, "gates": {}})

    with pytest.raises(ValueError, match="Invalid quality config"):
        parse_quality_config({"version": 2})


def test_rules_must_point_at_contract_tables_and_columns() -> None:
    validate_quality_config(_config({"beds": {"columns": {"rate": [{"min": 0}]}}}), [CONTRACT])

    with pytest.raises(ValueError, match="names table 'cars'"):
        validate_quality_config(_config({"cars": {}}), [CONTRACT])

    with pytest.raises(ValueError, match="names column 'Count'"):
        validate_quality_config(_config({"beds": {"columns": {"Count": ["not_null"]}}}), [CONTRACT])

    with pytest.raises(ValueError, match="names column 'year'"):
        validate_quality_config(_config({"beds": {"unique": ["year"]}}), [CONTRACT])


def test_numeric_rules_need_numeric_columns() -> None:
    config = _config({"beds": {"columns": {"geography_name": [{"min": 0}]}}})

    with pytest.raises(ValueError, match="needs a numeric column"):
        validate_quality_config(config, [CONTRACT])


def test_config_hash_changes_only_when_rules_change() -> None:
    first = _config({"beds": {"columns": {"rate": [{"min": 0}]}}})
    same = _config({"beds": {"columns": {"rate": [{"min": 0}]}}})
    stricter = _config({"beds": {"columns": {"rate": [{"min": 1}]}}})

    assert calculate_quality_config_hash(first) == calculate_quality_config_hash(same)
    assert calculate_quality_config_hash(first) != calculate_quality_config_hash(stricter)
