"""
File marshal: governance for ingested files.

We track raw ingested files, enforce a few invariants and keep an append-only
event log for auditing.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Literal

from metrka_core.metadata.file_marshal_errors import DuplicateSourceFileError
from metrka_core.metadata.file_marshal_models import (
    BronzeArtifactDigest,
    MarshaledFile,
    MarshalEntry,
    MarshalEvent,
)
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.pipeline.runtime_services import Clock

_STORED_FILE_RE = re.compile(
    r"^(?P<timestamp>\d{8}T\d{6}Z)_"
    r"(?P<file_id>[A-Za-z0-9-]+)_"
    r"(?P<original_filename>.+)$"
)


def get_original_filename(stored_filename: str | None) -> str | None:
    """Recover the original name from a stored landing filename."""

    if not stored_filename:
        return None

    name = Path(stored_filename).name
    match = _STORED_FILE_RE.match(name)

    if match is None:
        return name

    return match.group("original_filename")


class FileMarshal:
    """Coordinates file registration, promotion, supersession and audit logging."""

    def __init__(self, store: PostgresFileMarshalStore, *, clock: Clock) -> None:
        if store is None:
            raise ValueError("store is missing. Cannot initialize FileMarshal without persistence.")

        if clock is None:
            raise ValueError(
                "clock is missing. Cannot initialize FileMarshal without time services."
            )

        self._store = store
        self._clock = clock

    def get(self, file_id: str) -> MarshalEntry | None:
        """Fetch one entry directly from the database."""
        return self._store.get_marshaled_file(file_id)

    def get_by_hash(self, dataset_id: str, source_hash: str) -> MarshalEntry | None:
        """Fetch the registered file for one dataset and content hash."""
        return self._store.get_marshaled_file_by_hash(dataset_id, source_hash)

    def register(self, file: MarshaledFile, meta: dict[str, Any] | None = None) -> None:
        """Register a file (non-promoted by default). Meta should include the compute 'run_id'."""

        if self.get(file.dataset_file_id) is not None:
            raise ValueError("dataset_file_id already exists in FileMarshal catalog")

        if self._store.check_hash_exists(file.dataset_id, file.source_hash):
            raise DuplicateSourceFileError(dataset_id=file.dataset_id, source_hash=file.source_hash)

        entry = MarshalEntry(
            file=file,
            bronze_run_id=(meta or {}).get("bronze_run_id"),
            landing_path=(meta or {}).get("landing_path"),
        )

        event_meta = {"dataset_id": file.dataset_id, **(meta or {})}

        with self._store.transaction():
            self._put(file_id=file.dataset_file_id, new=entry, reason="register", meta=event_meta)

    def record_bronze_artifacts(
        self,
        file_id: str,
        artifacts: tuple[BronzeArtifactDigest, ...],
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Persist the immutable manifest of files produced by one Bronze run."""

        with self._store.transaction():
            target = self._require_entry(file_id)
            updated = target.with_bronze_artifacts(artifacts)
            self._put(
                file_id=file_id,
                new=updated,
                reason="record_bronze_artifacts",
                meta={
                    **(meta or {}),
                    "dataset_id": target.file.dataset_id,
                    "bronze_run_id": target.bronze_run_id,
                    "bronze_artifact_count": len(artifacts),
                    "bronze_artifact_bytes": sum(artifact.size_bytes for artifact in artifacts),
                },
            )

    def promote(
        self, file_id: str, version_period: date, meta: dict[str, Any] | None = None
    ) -> None:
        """Promote a single file for a discovered version_period."""

        with self._store.transaction():
            target = self._require_entry(file_id)
            dataset_id = target.file.dataset_id

            existing = self._store.get_promoted_for_version_period(dataset_id, version_period)

            if existing is not None and existing.file.dataset_file_id != file_id:
                self._put(
                    file_id=existing.file.dataset_file_id,
                    new=existing.demote(),
                    reason="promote_demote_previous",
                    meta={
                        **(meta or {}),
                        "dataset_id": dataset_id,
                        "version_period": version_period.isoformat(),
                        "new_promoted": file_id,
                    },
                )

            self._put(
                file_id=file_id,
                new=target.as_promoted(
                    self._clock.now_utc(),
                    version_period,
                    silver_run_id=(meta or {}).get("silver_run_id"),
                    manifest_path=(meta or {}).get("manifest_path"),
                    partition_key=(meta or {}).get("partition_key"),
                    partition_value=(meta or {}).get("partition_value"),
                ),
                reason="promote",
                meta={
                    **(meta or {}),
                    "dataset_id": dataset_id,
                    "version_period": version_period.isoformat(),
                },
            )

    def supersede(self, old_id: str, new_id: str) -> None:
        """Mark old file as superseded by new file."""
        if old_id == new_id:
            raise ValueError("file cannot supersede itself")

        with self._store.transaction():
            old_entry = self._require_entry(old_id)
            new_entry = self._require_entry(new_id)

            if old_entry.superseded_by_file_id is not None:
                raise ValueError("old file is already superseded")
            if new_entry.superseded_by_file_id is not None:
                raise ValueError("new file is superseded; cannot supersede others")

            if old_entry.file.dataset_id != new_entry.file.dataset_id:
                raise ValueError("supersede requires same dataset_id")

            if (
                old_entry.version_period
                and new_entry.version_period
                and old_entry.version_period != new_entry.version_period
            ):
                raise ValueError("supersede requires same version_period")

            self._put(
                file_id=old_id,
                new=old_entry.superseded_by(new_id),
                reason="supersede",
                meta={"dataset_id": old_entry.file.dataset_id, "superseded_by": new_id},
            )

    def _require_entry(self, file_id: str) -> MarshalEntry:
        """Get an entry or raise."""
        entry = self.get(file_id)
        if entry is None:
            raise ValueError("file not found in FileMarshal.")
        return entry

    def _put(self, file_id: str, new: MarshalEntry, reason: str, meta: dict[str, Any]) -> None:
        """Write path for entries + audit log + database hook."""
        old = self.get(file_id)
        event_type: Literal["entry_created", "entry_replaced"] = (
            "entry_created" if old is None else "entry_replaced"
        )

        event = MarshalEvent(
            event_ts=self._clock.now_utc(),
            event_type=event_type,
            file_id=file_id,
            reason=reason,
            old=asdict(old) if old else None,
            new=asdict(new),
            meta=dict(meta),
        )

        self._store.upsert_marshaled_file(new)
        self._store.insert_marshal_event(event)
