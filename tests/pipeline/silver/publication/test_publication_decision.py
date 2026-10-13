from __future__ import annotations

import pytest

from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
    SilverDatasetFingerprint,
)
from metrka_core.pipeline.silver.publication_decision import (
    SilverPublicationChangeKind,
    SilverPublicationDecisionStatus,
    decide_silver_publication,
)

from .fakes import make_publication


def _fingerprint(
    *,
    data_hash: str = "d" * 64,
    schema_hash: str = "e" * 64,
    version: int = 1,
    logical_hash_algorithm: str = LOGICAL_DATA_HASH_ALGORITHM,
    schema_hash_algorithm: str = SCHEMA_HASH_ALGORITHM,
) -> SilverDatasetFingerprint:
    return SilverDatasetFingerprint(
        logical_data_hash=data_hash,
        schema_hash=schema_hash,
        tables=(),
        fingerprint_version=version,
        logical_hash_algorithm=logical_hash_algorithm,
        schema_hash_algorithm=schema_hash_algorithm,
    )


def test_first_publication_requires_approval() -> None:
    decision = decide_silver_publication(
        current_publication=None, candidate_fingerprint=_fingerprint()
    )

    assert decision.status is SilverPublicationDecisionStatus.AWAITING_APPROVAL
    assert decision.change_kind is SilverPublicationChangeKind.INITIAL_PUBLICATION
    assert decision.baseline_publication_id is None


def test_equivalent_build_creates_verification_decision() -> None:
    publication = make_publication(logical_data_hash="d" * 64, schema_hash="e" * 64)

    decision = decide_silver_publication(
        current_publication=publication, candidate_fingerprint=_fingerprint()
    )

    assert decision.verified_equivalent
    assert decision.change_kind is SilverPublicationChangeKind.NONE
    assert decision.baseline_publication_id == publication.publication_id


@pytest.mark.parametrize(
    ("data_hash", "schema_hash", "expected"),
    [
        ("f" * 64, "e" * 64, SilverPublicationChangeKind.LOGICAL_DATA_CHANGED),
        ("d" * 64, "f" * 64, SilverPublicationChangeKind.SCHEMA_CHANGED),
        ("f" * 64, "f" * 64, SilverPublicationChangeKind.LOGICAL_DATA_AND_SCHEMA_CHANGED),
    ],
)
def test_changed_output_requires_approval(
    data_hash: str, schema_hash: str, expected: SilverPublicationChangeKind
) -> None:
    publication = make_publication()

    decision = decide_silver_publication(
        current_publication=publication,
        candidate_fingerprint=_fingerprint(data_hash=data_hash, schema_hash=schema_hash),
    )

    assert decision.requires_approval
    assert decision.change_kind is expected


def test_fingerprint_version_change_requires_approval() -> None:
    decision = decide_silver_publication(
        current_publication=make_publication(fingerprint_version=1),
        candidate_fingerprint=_fingerprint(version=2),
    )

    assert decision.change_kind is (SilverPublicationChangeKind.FINGERPRINT_VERSION_CHANGED)


def test_fingerprint_algorithm_change_is_not_reported_as_schema_change() -> None:
    decision = decide_silver_publication(
        current_publication=make_publication(),
        candidate_fingerprint=_fingerprint(
            schema_hash_algorithm="metrka.logical-schema.sha256.future"
        ),
    )

    assert decision.requires_approval
    assert decision.change_kind is SilverPublicationChangeKind.FINGERPRINT_ALGORITHM_CHANGED
