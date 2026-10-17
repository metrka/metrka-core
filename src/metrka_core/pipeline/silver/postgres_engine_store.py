"""PostgreSQL persistence for Silver engine releases."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from metrka_core.metadata.postgres import PostgresSession, to_jsonb
from metrka_core.pipeline.silver.engine_models import (
    SilverEngineIdentity,
    SilverEngineRelease,
    SilverEngineReleaseStatus,
)
from metrka_core.pipeline.silver.engine_store import (
    DEFAULT_ENGINE_RELEASE_LIST_LIMIT,
    require_engine_release_list_limit,
)

ENGINE_RELEASE_COLUMNS = """
    engine_release_id,
    release_hash,
    engine_hash,
    engine_fingerprint_version,
    runtime_hash,
    runtime_fingerprint_version,
    component_hashes,
    runtime_versions,
    core_commit_sha,
    status,
    detected_at,
    approved_at,
    approved_by,
    rejected_at,
    rejected_by,
    rejection_reason
"""


def _optional_string(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def _row_to_release(row: Any) -> SilverEngineRelease:
    record = dict(row)

    identity = SilverEngineIdentity(
        release_hash=str(record["release_hash"]),
        engine_hash=str(record["engine_hash"]),
        engine_fingerprint_version=int(record["engine_fingerprint_version"]),
        runtime_hash=str(record["runtime_hash"]),
        runtime_fingerprint_version=int(record["runtime_fingerprint_version"]),
        component_hashes=dict(record["component_hashes"]),
        runtime_versions=dict(record["runtime_versions"]),
    )

    return SilverEngineRelease(
        engine_release_id=str(record["engine_release_id"]),
        identity=identity,
        core_commit_sha=str(record["core_commit_sha"]),
        status=SilverEngineReleaseStatus(str(record["status"])),
        detected_at=record["detected_at"],
        approved_at=record["approved_at"],
        approved_by=_optional_string(record["approved_by"]),
        rejected_at=record["rejected_at"],
        rejected_by=_optional_string(record["rejected_by"]),
        rejection_reason=_optional_string(record["rejection_reason"]),
    )


class PostgresSilverEngineReleaseStore:
    """Persist and govern Silver engine releases."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def register_candidate(
        self, *, identity: SilverEngineIdentity, core_commit_sha: str, detected_at: datetime
    ) -> SilverEngineRelease:
        if detected_at.utcoffset() is None:
            raise ValueError("detected_at must be timezone-aware")

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meta.silver_engine_releases (
                    engine_release_id,
                    release_hash,
                    engine_hash,
                    engine_fingerprint_version,
                    runtime_hash,
                    runtime_fingerprint_version,
                    component_hashes,
                    runtime_versions,
                    core_commit_sha,
                    status,
                    detected_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    'candidate', %s
                )
                ON CONFLICT (release_hash) DO NOTHING
                """,
                (
                    identity.engine_release_id,
                    identity.release_hash,
                    identity.engine_hash,
                    identity.engine_fingerprint_version,
                    identity.runtime_hash,
                    identity.runtime_fingerprint_version,
                    to_jsonb(dict(identity.component_hashes)),
                    to_jsonb(dict(identity.runtime_versions)),
                    core_commit_sha,
                    detected_at,
                ),
            )

            cursor.execute(
                f"""
                SELECT
                    {ENGINE_RELEASE_COLUMNS}
                FROM meta.silver_engine_releases
                WHERE release_hash = %s
                """,
                (identity.release_hash,),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Silver engine release was not registered")

        return _row_to_release(row)

    def get_by_id(self, engine_release_id: str) -> SilverEngineRelease | None:
        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {ENGINE_RELEASE_COLUMNS}
                FROM meta.silver_engine_releases
                WHERE engine_release_id = %s
                """,
                (engine_release_id,),
            )

            row = cursor.fetchone()

        return _row_to_release(row) if row is not None else None

    def find_approved(self) -> SilverEngineRelease | None:
        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {ENGINE_RELEASE_COLUMNS}
                FROM meta.silver_engine_releases
                WHERE status = 'approved'
                LIMIT 1
                """
            )

            row = cursor.fetchone()

        return _row_to_release(row) if row is not None else None

    def list_releases(
        self, *, limit: int = DEFAULT_ENGINE_RELEASE_LIST_LIMIT
    ) -> list[SilverEngineRelease]:
        resolved_limit = require_engine_release_list_limit(limit)

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {ENGINE_RELEASE_COLUMNS}
                FROM meta.silver_engine_releases
                ORDER BY detected_at DESC, engine_release_id DESC
                LIMIT %s
                """,
                (resolved_limit,),
            )

            rows = cursor.fetchall()

        return [_row_to_release(row) for row in rows]

    def approve(
        self, *, engine_release_id: str, approved_by: str, approved_at: datetime
    ) -> SilverEngineRelease:
        if not approved_by.strip():
            raise ValueError("approved_by must not be empty")

        if approved_at.utcoffset() is None:
            raise ValueError("approved_at must be timezone-aware")

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_advisory_xact_lock(
                    hashtextextended(
                        'silver-engine-approval',
                        0
                    )
                )
                """
            )

            cursor.execute(
                """
                SELECT status
                FROM meta.silver_engine_releases
                WHERE engine_release_id = %s
                FOR UPDATE
                """,
                (engine_release_id,),
            )

            if cursor.fetchone() is None:
                raise RuntimeError(f"Silver engine release does not exist: {engine_release_id}")

            cursor.execute(
                """
                UPDATE meta.silver_engine_releases
                SET status = 'retired'
                WHERE status = 'approved'
                  AND engine_release_id <> %s
                """,
                (engine_release_id,),
            )

            cursor.execute(
                f"""
                UPDATE meta.silver_engine_releases
                SET
                    status = 'approved',
                    approved_at = %s,
                    approved_by = %s,
                    rejected_at = NULL,
                    rejected_by = NULL,
                    rejection_reason = NULL
                WHERE engine_release_id = %s
                RETURNING
                    {ENGINE_RELEASE_COLUMNS}
                """,
                (approved_at, approved_by.strip(), engine_release_id),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Silver engine release was not approved")

        return _row_to_release(row)

    def reject(
        self,
        *,
        engine_release_id: str,
        rejected_by: str,
        rejection_reason: str,
        rejected_at: datetime,
    ) -> SilverEngineRelease:
        if not rejected_by.strip():
            raise ValueError("rejected_by must not be empty")

        if not rejection_reason.strip():
            raise ValueError("rejection_reason must not be empty")

        if rejected_at.utcoffset() is None:
            raise ValueError("rejected_at must be timezone-aware")

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE meta.silver_engine_releases
                SET
                    status = 'rejected',
                    rejected_at = %s,
                    rejected_by = %s,
                    rejection_reason = %s
                WHERE engine_release_id = %s
                  AND status <> 'approved'
                RETURNING
                    {ENGINE_RELEASE_COLUMNS}
                """,
                (rejected_at, rejected_by.strip(), rejection_reason.strip(), engine_release_id),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError(
                "Release does not exist or is currently "
                "approved. Approve another release before "
                "rejecting it."
            )

        return _row_to_release(row)
