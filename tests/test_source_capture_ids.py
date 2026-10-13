"""Tests for source-capture identifier generation."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

import pytest

from metrka_core.pipeline.runtime_services import UuidSourceCaptureIdGenerator


def test_uuid_source_capture_id_contains_capture_time() -> None:
    generator = UuidSourceCaptureIdGenerator()

    source_capture_id = generator.new_source_capture_id(
        captured_at=datetime(2026, 8, 14, 17, 30, 45, tzinfo=UTC)
    )

    assert re.fullmatch(
        (
            r"capture_"
            r"20260814T173045Z_"
            r"[0-9a-f]{8}"
        ),
        source_capture_id,
    )


def test_uuid_source_capture_id_requires_utc() -> None:
    generator = UuidSourceCaptureIdGenerator()

    with pytest.raises(ValueError, match="UTC"):
        generator.new_source_capture_id(
            captured_at=datetime(2026, 8, 14, 19, 30, 45, tzinfo=timezone(timedelta(hours=2)))
        )
