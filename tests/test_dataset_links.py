"""Names shared by main.yaml, the Silver contract and quality.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from metrka_core.catalog.temporal_coverage import (
    calculate_temporal_coverage,
    parse_temporal_coverage_spec,
)
from metrka_core.datasets.source_config import load_source_config
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.silver.dataset_links import match_bronze_tables, unmatched_bronze_message
from metrka_core.pipeline.workspace_validation import validate_workspace
from metrka_core.quality.config import parse_quality_config, validate_quality_config

STREAM = "records_from_the_source"


def _main_yaml(**stream_overrides: Any) -> dict[str, Any]:
    """An anchored main.yaml: no contract name, Bronze file name or coverage table."""

    stream: dict[str, Any] = {
        "official_filename": "records_*.xlsx",
        "bronze_assembly": {"strategy": "xlsx_rows"},
        "silver": {
            "partition_by": "version_period",
            "version_period": {"strategy": "max_column", "grain": "year", "column": "Year"},
            "temporal_coverage": {"column": "reporting_year", "grain": "year"},
            "input": {"format": "csv"},
            "outputs": ["csv"],
        },
    }
    stream.update(stream_overrides)
    return {
        "workspace_name": "src",
        "streams": {STREAM: stream},
        "pipeline": {
            "quality": {"config": "quality.yaml"},
            "acquisition": {
                "extractor": "http.files",
                "options": {},
                "backfill": {
                    "source_url": "https://example.test/records.xlsx",
                    "match_mode": "pattern",
                    "source_last_modified_from": "target_date",
                },
            },
            "steps": [{"action": "bronze.ingest"}, {"action": "silver.process"}],
        },
    }


def _table(*columns: tuple[str, str, str]) -> dict[str, Any]:
    return {
        "columns": {
            source: {"rename_to": published, "cast_to": cast_to}
            for source, published, cast_to in columns
        },
        "canonical_order": [published for _, published, _ in columns],
    }


RECORDS_TABLE = _table(("County", "geography_name", "string"), ("Year", "reporting_year", "int"))


def _contract(tables: dict[str, Any]) -> dict[str, Any]:
    return {
        "meta": {"version": "1", "category": "health-medicine", "tags": ["test"]},
        "tables": tables,
    }


def _validate(
    tmp_path: Path,
    *,
    main: dict[str, Any],
    tables: dict[str, Any],
    quality: dict[str, Any] | None = None,
) -> Any:
    folder = tmp_path / "src" / STREAM
    conf = folder / "conf"
    conf.mkdir(parents=True)
    (conf / "main.yaml").write_text(yaml.safe_dump(main), encoding="utf-8")
    (conf / f"{STREAM}.yaml").write_text(yaml.safe_dump(_contract(tables)), encoding="utf-8")
    (conf / "quality.yaml").write_text(yaml.safe_dump(quality or {"version": 1}), encoding="utf-8")
    registry = tmp_path / "workspaces.local.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "workspaces": {
                    "src": {
                        "placement": "portable",
                        "workspace_root": str(tmp_path / "src"),
                        "layout": "dataset_folders",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return validate_workspace(
        f"src.{STREAM}",
        workspaces_config_path=registry,
        runtime_environment=RuntimeEnvironment.DEVELOPMENT,
    )


def test_anchored_dataset_needs_no_repeated_names(tmp_path: Path) -> None:
    result = _validate(
        tmp_path,
        main=_main_yaml(),
        tables={"records": RECORDS_TABLE},
        quality={"version": 1, "columns": {"geography_name": ["not_null"]}},
    )

    assert result.silver_table_names == ("records",)
    assert result.quality_check_count == 1


def test_stream_names_its_contract_and_bronze_file_by_default(tmp_path: Path) -> None:
    path = tmp_path / "main.yaml"
    path.write_text(yaml.safe_dump(_main_yaml()), encoding="utf-8")

    stream = load_source_config(path).streams[STREAM]

    assert stream.yaml_contract_name == f"{STREAM}.yaml"
    assert stream.bronze_assembly is not None
    assert stream.bronze_assembly.output_filename == f"{STREAM}.csv"


def test_bronze_file_must_feed_a_table_of_a_multi_table_contract(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError) as raised:
        _validate(
            tmp_path,
            main=_main_yaml(
                silver={
                    **_main_yaml()["streams"][STREAM]["silver"],
                    "temporal_coverage": {
                        "table": "records",
                        "column": "reporting_year",
                        "grain": "year",
                    },
                }
            ),
            tables={"records": RECORDS_TABLE, "notes": RECORDS_TABLE},
        )

    message = str(raised.value)
    assert f"Bronze file '{STREAM}.csv'" in message
    assert "['notes', 'records']" in message


def test_every_mismatch_is_reported_with_both_names(tmp_path: Path) -> None:
    silver = {
        **_main_yaml()["streams"][STREAM]["silver"],
        # Version periods are read from Bronze, before renaming.
        "version_period": {"strategy": "max_column", "grain": "year", "column": "reporting_year"},
        # Coverage is read from the finished table, after renaming.
        "temporal_coverage": {"table": "records", "column": "Year", "grain": "year"},
    }
    catalog = {
        "highlights": [
            {
                "key": "counties",
                "label": "Counties",
                "calculation": "distinct_count",
                "table": "other_dataset",
                "column": "geography_name",
            }
        ]
    }

    with pytest.raises(RuntimeError) as raised:
        _validate(
            tmp_path,
            main=_main_yaml(silver=silver, catalog=catalog),
            tables={"records": RECORDS_TABLE},
        )

    message = str(raised.value)
    assert "silver.version_period.column 'reporting_year' is not a Bronze column" in message
    assert "silver.temporal_coverage.column 'Year' is not a published column" in message
    assert "catalog.highlights[0].table 'other_dataset' is not a contract table" in message


def test_coverage_without_table_needs_a_one_table_contract(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="needs 'table' because the contract has several"):
        _validate(
            tmp_path,
            main=_main_yaml(bronze_assembly={"strategy": "xlsx_rows", "output_filename": "a.csv"}),
            tables={"a": RECORDS_TABLE, "b": RECORDS_TABLE},
        )


def test_bronze_file_feeds_the_table_named_like_it(tmp_path: Path) -> None:
    (tmp_path / "records.csv").write_text("x\n", encoding="utf-8")
    (tmp_path / "other.csv").write_text("x\n", encoding="utf-8")

    assert match_bronze_tables(tmp_path, ["records", "notes"]) == (
        ("records", tmp_path / "records.csv"),
    )


def test_one_bronze_file_feeds_the_only_table_whatever_its_name(tmp_path: Path) -> None:
    (tmp_path / "records_from_the_source.csv").write_text("x\n", encoding="utf-8")

    assert match_bronze_tables(tmp_path, ["records"]) == (
        ("records", tmp_path / "records_from_the_source.csv"),
    )


def test_unmatched_files_are_not_guessed_for_several_tables(tmp_path: Path) -> None:
    (tmp_path / "records_from_the_source.csv").write_text("x\n", encoding="utf-8")

    assert match_bronze_tables(tmp_path, ["records", "notes"]) == ()
    message = unmatched_bronze_message(
        dataset_id="src.records", bronze_dir=tmp_path, table_keys=["records", "notes"]
    )
    assert "Bronze files ['records_from_the_source.csv']" in message
    assert "contract tables ['notes', 'records']" in message


def test_top_level_quality_rules_apply_to_the_only_table() -> None:
    config = parse_quality_config({"version": 1, "columns": {"geography_name": ["not_null"]}})

    validate_quality_config(config, [_contract({"records": RECORDS_TABLE})])

    rules = config.rules_for("records")
    assert rules is not None
    assert list(rules.columns) == ["geography_name"]


def test_top_level_quality_rules_need_a_one_table_contract() -> None:
    config = parse_quality_config({"version": 1, "columns": {"geography_name": ["not_null"]}})

    with pytest.raises(ValueError, match="needs exactly one contract table"):
        validate_quality_config(config, [_contract({"a": RECORDS_TABLE, "b": RECORDS_TABLE})])


def test_quality_rules_are_written_one_way_only() -> None:
    with pytest.raises(ValueError, match="not both"):
        parse_quality_config(
            {
                "version": 1,
                "columns": {"geography_name": ["not_null"]},
                "tables": {"records": {"columns": {"geography_name": ["not_null"]}}},
            }
        )


def test_coverage_without_table_reads_the_only_committed_table(tmp_path: Path) -> None:
    data_file = tmp_path / "records" / "version_period=2025" / "data.csv"
    data_file.parent.mkdir(parents=True)
    data_file.write_text("reporting_year\n2002\n2003\n", encoding="utf-8")
    spec = parse_temporal_coverage_spec({"column": "reporting_year", "grain": "year"})

    coverage = calculate_temporal_coverage(spec=spec, data_files=[data_file], tables_root=tmp_path)

    assert coverage["source"] == {"table_key": "records", "column": "reporting_year"}
    assert coverage["minimum"] == 2002


def test_validation_lists_the_glossary_terms_the_contract_uses(tmp_path: Path) -> None:
    glossary = tmp_path / "src" / "glossary.yaml"
    glossary.parent.mkdir(parents=True)
    glossary.write_text("version: 1\nterms:\n  adult: Age 18 or older.\n", encoding="utf-8")

    result = _validate(
        tmp_path,
        main=_main_yaml(),
        tables={"records": {**RECORDS_TABLE, "info": "Counts for [[adult|adults]]."}},
    )

    assert result.glossary_terms == ("adult",)


def test_validation_rejects_an_undefined_term(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"\[\[adult\]\] is not defined in any glossary"):
        _validate(
            tmp_path,
            main=_main_yaml(),
            tables={"records": {**RECORDS_TABLE, "info": "Counts for [[adult]]."}},
        )


def test_a_dataset_folder_uses_a_profile_from_its_source(tmp_path: Path) -> None:
    profile = tmp_path / "src" / "profiles" / "records_shape.yaml"
    profile.parent.mkdir(parents=True)
    profile.write_text(yaml.safe_dump({"version": 1, "table": RECORDS_TABLE}), encoding="utf-8")

    result = _validate(
        tmp_path,
        main=_main_yaml(),
        tables={"records": {"uses": "records_shape", "display_name": "Records"}},
    )

    assert result.silver_table_names == ("records",)
