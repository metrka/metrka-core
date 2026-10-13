from __future__ import annotations

from unittest.mock import MagicMock

from metrka_core.pipeline.silver.build_models import SilverBuildStatus
from metrka_core.pipeline.silver.fingerprints import (
    LOGICAL_DATA_HASH_ALGORITHM,
    SCHEMA_HASH_ALGORITHM,
    SilverDatasetFingerprint,
)
from metrka_core.pipeline.silver.postgres_publication_decision_unit_of_work import (
    PostgresSilverPublicationDecisionUnitOfWork,
)
from metrka_core.pipeline.silver.publication_decision import SilverPublicationChangeKind
from metrka_core.pipeline.silver.publication_decision_unit_of_work import (
    SilverPublicationDecisionCommand,
)
from metrka_core.quality.publication_verification_models import SilverPublicationVerification

from .fakes import (
    DATASET_ID,
    FIXED_NOW,
    LOGICAL_HASH,
    QUALITY_HASH,
    SCHEMA_HASH,
    make_build,
    make_publication,
)

ENGINE_HASH = "9" * 64
OUTPUT_HASH = "8" * 64


def _session() -> MagicMock:
    session = MagicMock()
    session.transaction.return_value.__enter__.return_value = None
    session.cursor.return_value.__enter__.return_value = MagicMock()
    return session


def _fingerprint(
    *, logical_data_hash: str = LOGICAL_HASH, schema_hash: str = SCHEMA_HASH
) -> SilverDatasetFingerprint:
    return SilverDatasetFingerprint(
        logical_data_hash=logical_data_hash, schema_hash=schema_hash, tables=()
    )


def _command(*, fingerprint: SilverDatasetFingerprint) -> SilverPublicationDecisionCommand:
    return SilverPublicationDecisionCommand(
        dataset_id=DATASET_ID,
        bronze_file_id="bronze-file-1",
        silver_build_id="build-1",
        engine_hash=ENGINE_HASH,
        quality_config_hash=QUALITY_HASH,
        version_period=make_publication().version_period,
        partition_key="version_period",
        partition_value="2025",
        manifest_path="data/files/silver/manifests/build-1.json",
        output_hash=OUTPUT_HASH,
        output_file_count=1,
        output_byte_count=12345,
        completed_at=FIXED_NOW,
        fingerprint=fingerprint,
        marshal_meta={"source": "test"},
    )


def test_equivalent_build_records_verification_without_candidate() -> None:
    session = _session()
    publication = make_publication()
    completed_build = make_build(status=SilverBuildStatus.SUCCEEDED, completed_at=FIXED_NOW)
    verification = SilverPublicationVerification(
        publication_id=publication.publication_id,
        engine_hash=ENGINE_HASH,
        logical_hash_algorithm=LOGICAL_DATA_HASH_ALGORITHM,
        schema_hash_algorithm=SCHEMA_HASH_ALGORITHM,
        latest_silver_build_id="build-1",
        quality_config_hash=QUALITY_HASH,
        verification_count=1,
        first_verified_at=FIXED_NOW,
        last_verified_at=FIXED_NOW,
    )
    builds = MagicMock()
    builds.mark_succeeded.return_value = completed_build
    marshal = MagicMock()
    publications = MagicMock()
    publications.find_active.return_value = publication
    verifications = MagicMock()
    verifications.record.return_value = verification
    candidates = MagicMock()
    candidate_ids = MagicMock()
    candidate_ids.new_publication_candidate_id.return_value = "publication-candidate-generated"
    unit = PostgresSilverPublicationDecisionUnitOfWork(
        session=session,
        silver_builds=builds,
        marshal=marshal,
        publications=publications,
        verifications=verifications,
        candidates=candidates,
        candidate_ids=candidate_ids,
    )

    result = unit.commit(_command(fingerprint=_fingerprint()))

    assert result.decision.verified_equivalent
    assert result.verification == verification
    assert result.candidate is None
    candidate_ids.new_publication_candidate_id.assert_not_called()
    verifications.record.assert_called_once()
    candidates.register.assert_not_called()
    marshal.promote.assert_called_once()
    session.transaction.assert_called_once()


def test_changed_build_registers_candidate_without_verification() -> None:
    session = _session()
    completed_build = make_build(
        status=SilverBuildStatus.SUCCEEDED, completed_at=FIXED_NOW, logical_data_hash="7" * 64
    )
    builds = MagicMock()
    builds.mark_succeeded.return_value = completed_build
    publications = MagicMock()
    publications.find_active.return_value = make_publication()
    verifications = MagicMock()
    candidates = MagicMock()
    candidate = MagicMock()
    candidates.register.return_value = candidate
    candidate_ids = MagicMock()
    candidate_ids.new_publication_candidate_id.return_value = "publication-candidate-generated"
    unit = PostgresSilverPublicationDecisionUnitOfWork(
        session=session,
        silver_builds=builds,
        marshal=MagicMock(),
        publications=publications,
        verifications=verifications,
        candidates=candidates,
        candidate_ids=candidate_ids,
    )

    result = unit.commit(_command(fingerprint=_fingerprint(logical_data_hash="7" * 64)))

    assert result.decision.requires_approval
    assert result.decision.change_kind is SilverPublicationChangeKind.LOGICAL_DATA_CHANGED
    assert result.candidate is candidate
    assert result.verification is None
    verifications.record.assert_not_called()
    candidate_request = candidates.register.call_args.args[0]
    assert candidate_request.candidate_id == "publication-candidate-generated"
    candidate_ids.new_publication_candidate_id.assert_called_once_with()
    assert candidate_request.baseline_publication_id == "publication-1"
    assert candidate_request.logical_data_hash == "7" * 64
