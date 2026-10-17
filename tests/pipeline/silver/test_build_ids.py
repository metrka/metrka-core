"""Tests for injectable Silver build identifiers."""

from __future__ import annotations

from uuid import UUID

from metrka_core.pipeline.runtime_services import UuidSilverBuildIdGenerator


def test_uuid_silver_build_ids_are_valid_and_unique() -> None:
    generator = UuidSilverBuildIdGenerator()

    first = generator.new_silver_build_id()
    second = generator.new_silver_build_id()

    assert first != second
    assert str(UUID(first)) == first
    assert str(UUID(second)) == second
    assert UUID(first).version == 4
    assert UUID(second).version == 4
