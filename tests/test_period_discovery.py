from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from metrka_core.metadata.file_marshal_models import MarshaledFile
from metrka_core.pipeline.silver.version_period import (
    VersionPeriodSpec,
    build_version_period_discovery,
    parse_version_period_spec,
)


def _marshaled_file(*, source_last_modified: datetime | None = None) -> MarshaledFile:
    return MarshaledFile(
        dataset_file_id="file-1",
        dataset_id="dataset-1",
        source_url="https://example.test/source.csv",
        source_file_name="stored-source.csv",
        original_source_file_name="source.csv",
        source_hash="hash-1",
        file_size=10,
        ingestion_timestamp=datetime(2026, 8, 13, tzinfo=UTC),
        source_last_modified=source_last_modified,
        row_count_raw=0,
        column_count_raw=1,
    )


def test_max_column_discovers_and_normalizes_year(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("id,report_date\n1,2024-05-01\n2,2025-09-15\n", encoding="utf-8")
    discover = build_version_period_discovery(
        spec=VersionPeriodSpec(
            strategy="max_column", grain="year", column="report_date", date_format="%Y-%m-%d"
        ),
        input_format="csv",
    )

    period = discover(source, {}, _marshaled_file())

    assert period.value.isoformat() == "2025-01-01"
    assert period.grain == "year"
    assert period.source == "column:report_date"


def test_tsv_discovery_uses_tab_separator(tmp_path: Path) -> None:
    source = tmp_path / "source.tsv"
    source.write_text("id\treport_date\n1\t2026-01-15\n2\t2026-03-29\n", encoding="utf-8")
    discover = build_version_period_discovery(
        spec=VersionPeriodSpec(strategy="max_column", grain="month", column="report_date"),
        input_format="tsv",
    )

    period = discover(source, {}, _marshaled_file())

    assert period.value.isoformat() == "2026-03-01"


def test_source_last_modified_strategy_is_explicit() -> None:
    modified = datetime(2026, 7, 18, tzinfo=UTC)
    discover = build_version_period_discovery(
        spec=VersionPeriodSpec(strategy="source_last_modified", grain="month"), input_format="csv"
    )

    period = discover(Path("unused.csv"), {}, _marshaled_file(source_last_modified=modified))

    assert period.value.isoformat() == "2026-07-01"
    assert period.source == "source_last_modified"


def test_invalid_dates_fail_instead_of_silently_using_ingestion_time(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("report_date\nnot-a-date\n", encoding="utf-8")
    discover = build_version_period_discovery(
        spec=VersionPeriodSpec(strategy="max_column", grain="year", column="report_date"),
        input_format="csv",
    )

    with pytest.raises(ValueError, match="Could not discover version period"):
        discover(source, {}, _marshaled_file())


def test_parse_version_period_rejects_unknown_keys() -> None:
    with pytest.raises(RuntimeError, match="Unsupported"):
        parse_version_period_spec(
            {
                "strategy": "max_column",
                "grain": "year",
                "column": "year",
                "fallback": "ingestion_timestamp",
            },
            stream_name="county",
        )
