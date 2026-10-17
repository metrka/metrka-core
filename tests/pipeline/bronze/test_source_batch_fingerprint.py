from pathlib import Path

from metrka_core.pipeline.bronze.source_batch_fingerprint import fingerprint_source_batch


def test_source_batch_fingerprint_is_order_independent(tmp_path: Path) -> None:
    first = tmp_path / "a.xlsx"
    second = tmp_path / "b.xlsx"

    first.write_bytes(b"first workbook")
    second.write_bytes(b"second workbook")

    forward = fingerprint_source_batch((first, second))
    reverse = fingerprint_source_batch((second, first))

    assert forward == reverse
    assert tuple(member.filename for member in forward.members) == ("a.xlsx", "b.xlsx")


def test_source_batch_fingerprint_changes_when_a_file_changes(tmp_path: Path) -> None:
    first = tmp_path / "a.xlsx"
    second = tmp_path / "b.xlsx"

    first.write_bytes(b"first workbook")
    second.write_bytes(b"second workbook")

    original = fingerprint_source_batch((first, second))

    second.write_bytes(b"changed second workbook")

    changed = fingerprint_source_batch((first, second))

    assert changed.sha256 != original.sha256
