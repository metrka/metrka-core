from __future__ import annotations

from unittest.mock import MagicMock

from metrka_core.quality.postgres_publication_gate_evidence_store import (
    PostgresPublicationGateEvidenceStore,
)
from metrka_core.quality.publication_gate_evidence_models import PublicationGateAttempt


def test_postgres_store_links_attempt_to_integrity_batch() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"gate_attempt_id": 42}
    attempt = PublicationGateAttempt(
        candidate_id="candidate-1",
        silver_build_id="build-1",
        pipeline_run_id="pipeline-1",
        integrity_batch_id=41,
    )

    gate_attempt_id = PostgresPublicationGateEvidenceStore(session).insert_attempt(attempt)

    assert gate_attempt_id == 42
    assert cursor.execute.call_count == 1
    attempt_parameters = cursor.execute.call_args_list[0].args[1]
    assert attempt_parameters == (
        attempt.candidate_id,
        attempt.silver_build_id,
        attempt.pipeline_run_id,
        attempt.integrity_batch_id,
    )
    session.transaction.assert_called_once_with()


def test_postgres_store_rejects_missing_generated_identity() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None
    attempt = PublicationGateAttempt(
        candidate_id="candidate-1",
        silver_build_id="build-1",
        pipeline_run_id="pipeline-1",
        integrity_batch_id=41,
    )

    try:
        PostgresPublicationGateEvidenceStore(session).insert_attempt(attempt)
    except RuntimeError as error:
        assert "gate_attempt_id" in str(error)
    else:
        raise AssertionError("Expected the missing database identity to be rejected")
