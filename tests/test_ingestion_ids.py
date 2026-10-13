"""Tests for source-file and Bronze run identifiers."""

from __future__ import annotations

from uuid import UUID

from metrka_core.pipeline.runtime_services import (
    UuidBronzeRunIdGenerator,
    UuidDatasetFileIdGenerator,
)


def test_dataset_file_id_is_uuid4() -> None:
    generator = UuidDatasetFileIdGenerator()

    first = generator.new_dataset_file_id()
    second = generator.new_dataset_file_id()

    assert first != second
    assert UUID(first).version == 4
    assert UUID(second).version == 4


def test_bronze_run_id_has_expected_format() -> None:
    generator = UuidBronzeRunIdGenerator()

    first = generator.new_bronze_run_id()
    second = generator.new_bronze_run_id()

    assert first != second
    assert first.startswith("bronze_")
    assert second.startswith("bronze_")
    assert len(first) == len("bronze_") + 12
