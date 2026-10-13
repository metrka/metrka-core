from __future__ import annotations

import pytest

from metrka_core.quality.publication_gate_evidence_models import PublicationGateAttempt


def test_gate_attempt_references_one_persisted_integrity_batch() -> None:
    attempt = PublicationGateAttempt(
        candidate_id="candidate-1",
        silver_build_id="build-1",
        pipeline_run_id="pipeline-1",
        integrity_batch_id=42,
    )

    assert attempt.integrity_batch_id == 42
    assert not hasattr(attempt, "results")


def test_gate_attempt_rejects_invalid_batch_identity() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        PublicationGateAttempt(
            candidate_id="candidate-1",
            silver_build_id="build-1",
            pipeline_run_id="pipeline-1",
            integrity_batch_id=0,
        )
