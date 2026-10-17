"""PostgreSQL persistence for publication verifications."""

from __future__ import annotations

from typing import Any

from metrka_core.metadata.postgres import PostgresSession
from metrka_core.quality.publication_verification_models import (
    SilverPublicationVerification,
    SilverPublicationVerificationRequest,
)

VERIFICATION_COLUMNS = """
    publication_id,
    engine_hash,
    logical_hash_algorithm,
    schema_hash_algorithm,
    latest_silver_build_id,
    quality_config_hash,
    verification_count,
    first_verified_at,
    last_verified_at
"""


def _row_to_verification(row: Any) -> SilverPublicationVerification:
    record = dict(row)

    return SilverPublicationVerification(
        publication_id=str(record["publication_id"]),
        engine_hash=str(record["engine_hash"]),
        logical_hash_algorithm=str(record["logical_hash_algorithm"]),
        schema_hash_algorithm=str(record["schema_hash_algorithm"]),
        latest_silver_build_id=str(record["latest_silver_build_id"]),
        quality_config_hash=str(record["quality_config_hash"]),
        verification_count=int(record["verification_count"]),
        first_verified_at=record["first_verified_at"],
        last_verified_at=record["last_verified_at"],
    )


class PostgresSilverPublicationVerificationStore:
    """Persist aggregated reproducibility evidence."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def record(
        self, request: SilverPublicationVerificationRequest
    ) -> SilverPublicationVerification:
        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO
                    quality.silver_publication_verifications
                    AS existing (
                        publication_id,
                        engine_hash,
                        logical_hash_algorithm,
                        schema_hash_algorithm,
                        latest_silver_build_id,
                        quality_config_hash,
                        verification_count,
                        first_verified_at,
                        last_verified_at
                    )
                VALUES (
                    %s, %s, %s, %s, %s, %s, 1, %s, %s
                )
                ON CONFLICT (
                    publication_id,
                    engine_hash,
                    logical_hash_algorithm,
                    schema_hash_algorithm
                )
                DO UPDATE SET
                    latest_silver_build_id = (
                        EXCLUDED.latest_silver_build_id
                    ),
                    quality_config_hash = CASE
                        WHEN (
                            existing.latest_silver_build_id
                            = EXCLUDED.latest_silver_build_id
                        )
                        THEN existing.quality_config_hash
                        ELSE EXCLUDED.quality_config_hash
                    END,
                    verification_count = CASE
                        WHEN (
                            existing.latest_silver_build_id
                            = EXCLUDED.latest_silver_build_id
                        )
                        THEN existing.verification_count
                        ELSE existing.verification_count + 1
                    END,
                    last_verified_at = CASE
                        WHEN (
                            existing.latest_silver_build_id
                            = EXCLUDED.latest_silver_build_id
                        )
                        THEN existing.last_verified_at
                        ELSE GREATEST(
                            existing.last_verified_at,
                            EXCLUDED.last_verified_at
                        )
                    END
                RETURNING
                    {VERIFICATION_COLUMNS}
                """,
                (
                    request.publication_id,
                    request.engine_hash,
                    request.logical_hash_algorithm,
                    request.schema_hash_algorithm,
                    request.silver_build_id,
                    request.quality_config_hash,
                    request.verified_at,
                    request.verified_at,
                ),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Publication verification was not recorded")

        return _row_to_verification(row)
