from __future__ import annotations

from pathlib import Path

import pytest

from metrka_core.storage.atomic_writes import atomic_write, atomic_write_bytes, atomic_write_text


def test_atomic_write_text_replaces_existing_content(tmp_path: Path) -> None:
    destination = tmp_path / "state.json"
    destination.write_text("old", encoding="utf-8")

    result = atomic_write_text(destination, "new")

    assert result == destination.resolve()
    assert destination.read_text(encoding="utf-8") == "new"
    assert list(tmp_path.glob(".*.tmp.json")) == []


def test_atomic_write_bytes_persists_binary_content(tmp_path: Path) -> None:
    destination = tmp_path / "artifact.bin"

    atomic_write_bytes(destination, b"\x00\x01\x02")

    assert destination.read_bytes() == b"\x00\x01\x02"


def test_writer_failure_preserves_previous_destination(tmp_path: Path) -> None:
    destination = tmp_path / "state.json"
    destination.write_text("old", encoding="utf-8")

    def fail_after_partial_write(temporary_path: Path) -> None:
        temporary_path.write_text("partial", encoding="utf-8")
        raise RuntimeError("writer failed")

    with pytest.raises(RuntimeError, match="writer failed"):
        atomic_write(destination, fail_after_partial_write)

    assert destination.read_text(encoding="utf-8") == "old"
    assert list(tmp_path.glob(".*.tmp.json")) == []
