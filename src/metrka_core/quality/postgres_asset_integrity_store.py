"""PostgreSQL adapter for normalized file-integrity evidence."""

from __future__ import annotations

from typing import Any

from metrka_core.metadata.postgres import PostgresSession, to_jsonb
from metrka_core.quality.asset_integrity_models import AssetIntegrityBatch
from metrka_core.quality.publication_integrity_models import (
    PublicationIntegrityBatchLink,
    PublicationIntegrityCheck,
)


class PostgresAssetIntegrityEvidenceStore:
    """Persist integrity facts once and link business events to them."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def insert_batch(self, batch: AssetIntegrityBatch) -> int:
        """Persist one batch independently of its business use."""

        with self._session.transaction(), self._session.cursor() as cursor:
            return self._insert_batch(cursor=cursor, batch=batch)

    def link_batch(self, link: PublicationIntegrityBatchLink) -> None:
        """Link a previously persisted batch to a publication."""

        with self._session.transaction(), self._session.cursor() as cursor:
            self._insert_publication_link(cursor=cursor, link=link)

    def insert_check(self, check: PublicationIntegrityCheck) -> int:
        """Persist a reconciliation batch and publication link atomically."""

        with self._session.transaction(), self._session.cursor() as cursor:
            integrity_batch_id = self._insert_batch(cursor=cursor, batch=check.batch)
            self._insert_publication_link(
                cursor=cursor,
                link=PublicationIntegrityBatchLink(
                    publication_id=check.publication_id,
                    trigger=check.trigger,
                    integrity_batch_id=integrity_batch_id,
                ),
            )

        return integrity_batch_id

    @staticmethod
    def _insert_batch(*, cursor: Any, batch: AssetIntegrityBatch) -> int:
        cursor.execute(
            """
            INSERT INTO quality.asset_integrity_batches (checked_at)
            VALUES (%s)
            RETURNING integrity_batch_id
            """,
            (batch.checked_at,),
        )
        row = cursor.fetchone()
        integrity_batch_id = None if row is None else row.get("integrity_batch_id")

        if (
            isinstance(integrity_batch_id, bool)
            or not isinstance(integrity_batch_id, int)
            or integrity_batch_id <= 0
        ):
            raise RuntimeError("Integrity batch insert returned no valid integrity_batch_id")

        for result in batch.results:
            cursor.execute(
                """
                INSERT INTO quality.asset_integrity_results (
                    integrity_batch_id,
                    file_path,
                    status,
                    expected_size_bytes,
                    actual_size_bytes,
                    expected_checksum,
                    actual_checksum,
                    failure_codes,
                    error_type,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    integrity_batch_id,
                    result.file_path,
                    result.status.value,
                    result.expected_size_bytes,
                    result.actual_size_bytes,
                    result.expected_checksum,
                    result.actual_checksum,
                    to_jsonb([code.value for code in result.failure_codes]),
                    result.error_type,
                    result.error_message,
                ),
            )

        return integrity_batch_id

    @staticmethod
    def _insert_publication_link(*, cursor: Any, link: PublicationIntegrityBatchLink) -> None:
        cursor.execute(
            """
            INSERT INTO quality.publication_integrity_checks (
                publication_id,
                integrity_batch_id,
                verification_trigger
            )
            VALUES (%s, %s, %s)
            """,
            (link.publication_id, link.integrity_batch_id, link.trigger.value),
        )
