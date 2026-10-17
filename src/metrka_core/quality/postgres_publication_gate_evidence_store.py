"""PostgreSQL adapter for append-only publication-gate evidence."""

from __future__ import annotations

from metrka_core.metadata.postgres import PostgresSession
from metrka_core.quality.publication_gate_evidence_models import PublicationGateAttempt


class PostgresPublicationGateEvidenceStore:
    """Link candidate decisions to normalized integrity evidence."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def insert_attempt(self, attempt: PublicationGateAttempt) -> int:
        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO quality.publication_gate_attempts (
                    candidate_id,
                    silver_build_id,
                    pipeline_run_id,
                    integrity_batch_id
                )
                VALUES (%s, %s, %s, %s)
                RETURNING gate_attempt_id
                """,
                (
                    attempt.candidate_id,
                    attempt.silver_build_id,
                    attempt.pipeline_run_id,
                    attempt.integrity_batch_id,
                ),
            )

            row = cursor.fetchone()
            gate_attempt_id = None if row is None else row.get("gate_attempt_id")

            if (
                isinstance(gate_attempt_id, bool)
                or not isinstance(gate_attempt_id, int)
                or gate_attempt_id <= 0
            ):
                raise RuntimeError("Publication gate insert returned no valid gate_attempt_id")

        return gate_attempt_id
