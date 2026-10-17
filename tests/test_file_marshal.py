from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime

import pytest

from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.file_marshal_errors import DuplicateSourceFileError
from metrka_core.metadata.file_marshal_models import (
    BronzeArtifactDigest,
    MarshaledFile,
    MarshalEntry,
    MarshalEvent,
)

FROZEN_TIME = datetime(2026, 8, 14, 12, 30, tzinfo=UTC)


class FrozenClock:
    """Return one deterministic UTC timestamp."""

    def now_utc(self) -> datetime:
        return FROZEN_TIME


class InMemoryMarshalStore:
    def __init__(self) -> None:
        self.entries: dict[str, MarshalEntry] = {}
        self.events: list[MarshalEvent] = []
        self.transactions = 0

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.transactions += 1
        yield

    def upsert_marshaled_file(self, entry: MarshalEntry) -> None:
        self.entries[entry.file.dataset_file_id] = entry

    def insert_marshal_event(self, event: MarshalEvent) -> None:
        self.events.append(event)

    def check_hash_exists(self, dataset_id: str, source_hash: str) -> bool:
        return self.get_marshaled_file_by_hash(dataset_id, source_hash) is not None

    def get_marshaled_file_by_hash(self, dataset_id: str, source_hash: str) -> MarshalEntry | None:
        return next(
            (
                entry
                for entry in self.entries.values()
                if entry.file.dataset_id == dataset_id and entry.file.source_hash == source_hash
            ),
            None,
        )

    def get_marshaled_file(self, dataset_file_id: str) -> MarshalEntry | None:
        return self.entries.get(dataset_file_id)

    def get_promoted_for_version_period(
        self, dataset_id: str, version_period: date
    ) -> MarshalEntry | None:
        return next(
            (
                entry
                for entry in self.entries.values()
                if entry.file.dataset_id == dataset_id
                and entry.version_period == version_period
                and entry.is_promoted
            ),
            None,
        )


def _file(
    file_id: str, *, dataset_id: str = "dataset-1", source_hash: str = "hash-1"
) -> MarshaledFile:
    return MarshaledFile(
        dataset_id=dataset_id,
        source_url="https://example.test/source.csv",
        source_file_name="stored-source.csv",
        original_source_file_name="source.csv",
        source_hash=source_hash,
        file_size=10,
        ingestion_timestamp=datetime(2026, 8, 13, tzinfo=UTC),
        source_last_modified=None,
        row_count_raw=0,
        column_count_raw=1,
        dataset_file_id=file_id,
    )


def test_file_marshal_requires_store() -> None:
    with pytest.raises(ValueError, match="store is missing"):
        FileMarshal(None, clock=FrozenClock())  # type: ignore[arg-type]


def test_register_persists_entry_and_event() -> None:
    store = InMemoryMarshalStore()
    marshal = FileMarshal(store, clock=FrozenClock())
    source = _file("file-1")

    marshal.register(source, meta={"bronze_run_id": "bronze-1"})

    entry = marshal.get("file-1")
    assert entry is not None
    assert entry.bronze_run_id == "bronze-1"
    assert store.events[0].reason == "register"
    assert store.events[0].event_ts == FROZEN_TIME
    assert store.transactions == 1


def test_register_reports_typed_duplicate_with_source_identity() -> None:
    marshal = FileMarshal(InMemoryMarshalStore(), clock=FrozenClock())
    first = _file("file-1")
    marshal.register(first)

    with pytest.raises(DuplicateSourceFileError) as captured:
        marshal.register(_file("file-2"))

    assert captured.value.dataset_id == first.dataset_id
    assert captured.value.source_hash == first.source_hash
    assert isinstance(captured.value, ValueError)


def test_record_bronze_artifacts_persists_manifest_and_audit_event() -> None:
    store = InMemoryMarshalStore()
    marshal = FileMarshal(store, clock=FrozenClock())
    marshal.register(_file("file-1"), meta={"bronze_run_id": "bronze-1"})
    artifacts = (BronzeArtifactDigest(relative_path="table.csv", sha256="a" * 64, size_bytes=42),)

    marshal.record_bronze_artifacts("file-1", artifacts)

    entry = marshal.get("file-1")
    assert entry is not None
    assert entry.bronze_artifacts == artifacts
    assert store.events[-1].reason == "record_bronze_artifacts"
    assert store.events[-1].new["bronze_artifacts"][0]["relative_path"] == "table.csv"


def test_record_bronze_artifacts_rejects_manifest_replacement() -> None:
    store = InMemoryMarshalStore()
    marshal = FileMarshal(store, clock=FrozenClock())
    marshal.register(_file("file-1"), meta={"bronze_run_id": "bronze-1"})
    marshal.record_bronze_artifacts(
        "file-1", (BronzeArtifactDigest(relative_path="table.csv", sha256="a" * 64, size_bytes=10),)
    )

    with pytest.raises(ValueError, match="immutable"):
        marshal.record_bronze_artifacts(
            "file-1",
            (BronzeArtifactDigest(relative_path="table.csv", sha256="b" * 64, size_bytes=10),),
        )


def test_promote_demotes_previous_file_for_same_period() -> None:
    store = InMemoryMarshalStore()
    marshal = FileMarshal(store, clock=FrozenClock())
    marshal.register(_file("file-1", source_hash="hash-1"))
    marshal.register(_file("file-2", source_hash="hash-2"))
    period = date(2025, 1, 1)

    marshal.promote("file-1", period)
    marshal.promote("file-2", period)

    first = marshal.get("file-1")
    second = marshal.get("file-2")
    assert first is not None and not first.is_promoted
    assert second is not None and second.is_promoted
    assert second.version_period == period
    assert second.promoted_at == FROZEN_TIME
    assert all(event.event_ts == FROZEN_TIME for event in store.events)
    assert [event.reason for event in store.events[-2:]] == ["promote_demote_previous", "promote"]


def test_superseded_file_cannot_be_promoted() -> None:
    marshal = FileMarshal(InMemoryMarshalStore(), clock=FrozenClock())
    marshal.register(_file("file-1", source_hash="hash-1"))
    marshal.register(_file("file-2", source_hash="hash-2"))
    marshal.supersede("file-1", "file-2")

    with pytest.raises(ValueError, match="superseded file cannot be promoted"):
        marshal.promote("file-1", date(2025, 1, 1))


def test_file_marshal_requires_clock() -> None:
    with pytest.raises(ValueError, match="clock is missing"):
        FileMarshal(
            InMemoryMarshalStore(),
            clock=None,  # type: ignore[arg-type]
        )
