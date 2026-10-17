"""PostgreSQL unit of work for Silver publication decisions."""

from __future__ import annotations

from metrka_core.catalog.postgres_publication_candidate_store import (
    PostgresDatasetPublicationCandidateStore,
)
from metrka_core.catalog.publication_candidate_models import DatasetPublicationCandidateRequest
from metrka_core.catalog.publication_store import DatasetPublicationStore
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.runtime_services import PublicationCandidateIdGenerator
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.publication_decision import decide_silver_publication
from metrka_core.pipeline.silver.publication_decision_unit_of_work import (
    SilverPublicationDecisionCommand,
    SilverPublicationDecisionResult,
)
from metrka_core.quality.postgres_publication_verification_store import (
    PostgresSilverPublicationVerificationStore,
)
from metrka_core.quality.publication_verification_models import SilverPublicationVerificationRequest


class PostgresSilverPublicationDecisionUnitOfWork:
    """Finalize a build and record its publication decision."""

    def __init__(
        self,
        *,
        session: PostgresSession,
        silver_builds: PostgresSilverBuildStore,
        marshal: FileMarshal,
        publications: DatasetPublicationStore,
        verifications: PostgresSilverPublicationVerificationStore,
        candidates: PostgresDatasetPublicationCandidateStore,
        candidate_ids: PublicationCandidateIdGenerator,
    ) -> None:
        self._session = session
        self._silver_builds = silver_builds
        self._marshal = marshal
        self._publications = publications
        self._verifications = verifications
        self._candidates = candidates
        self._candidate_ids = candidate_ids

    def commit(self, command: SilverPublicationDecisionCommand) -> SilverPublicationDecisionResult:
        """Finalize one build without automatically publishing it."""

        with self._session.transaction():
            self._lock_dataset(command.dataset_id)

            baseline_publication = self._publications.find_active(
                dataset_id=command.dataset_id, partition_value=command.partition_value
            )

            decision = decide_silver_publication(
                current_publication=baseline_publication, candidate_fingerprint=command.fingerprint
            )

            completed_build = self._silver_builds.mark_succeeded(
                silver_build_id=command.silver_build_id,
                version_period=command.version_period,
                partition_key=command.partition_key,
                partition_value=command.partition_value,
                manifest_path=command.manifest_path,
                output_hash=command.output_hash,
                output_file_count=command.output_file_count,
                output_byte_count=command.output_byte_count,
                fingerprint_version=command.fingerprint.fingerprint_version,
                logical_hash_algorithm=command.fingerprint.logical_hash_algorithm,
                schema_hash_algorithm=command.fingerprint.schema_hash_algorithm,
                logical_data_hash=command.fingerprint.logical_data_hash,
                schema_hash=command.fingerprint.schema_hash,
                completed_at=command.completed_at,
            )

            marshal_meta = dict(command.marshal_meta)
            marshal_meta.update(
                {
                    "publication_decision_status": decision.status.value,
                    "publication_change_kind": decision.change_kind.value,
                    "baseline_publication_id": decision.baseline_publication_id,
                }
            )

            self._marshal.promote(command.bronze_file_id, command.version_period, meta=marshal_meta)

            if decision.verified_equivalent:
                if baseline_publication is None:
                    raise RuntimeError(
                        "Equivalent publication decision requires a baseline publication"
                    )

                verification = self._verifications.record(
                    SilverPublicationVerificationRequest(
                        publication_id=baseline_publication.publication_id,
                        engine_hash=command.engine_hash,
                        logical_hash_algorithm=command.fingerprint.logical_hash_algorithm,
                        schema_hash_algorithm=command.fingerprint.schema_hash_algorithm,
                        silver_build_id=command.silver_build_id,
                        quality_config_hash=command.quality_config_hash,
                        verified_at=command.completed_at,
                    )
                )

                return SilverPublicationDecisionResult(
                    completed_build=completed_build, decision=decision, verification=verification
                )

            candidate = self._candidates.register(
                DatasetPublicationCandidateRequest(
                    candidate_id=self._candidate_ids.new_publication_candidate_id(),
                    dataset_id=command.dataset_id,
                    version_period=command.version_period,
                    partition_key=command.partition_key,
                    partition_value=command.partition_value,
                    silver_build_id=command.silver_build_id,
                    baseline_publication_id=decision.baseline_publication_id,
                    change_kind=decision.change_kind,
                    fingerprint_version=command.fingerprint.fingerprint_version,
                    logical_hash_algorithm=command.fingerprint.logical_hash_algorithm,
                    schema_hash_algorithm=command.fingerprint.schema_hash_algorithm,
                    logical_data_hash=command.fingerprint.logical_data_hash,
                    schema_hash=command.fingerprint.schema_hash,
                    requested_at=command.completed_at,
                )
            )

            return SilverPublicationDecisionResult(
                completed_build=completed_build, decision=decision, candidate=candidate
            )

    def _lock_dataset(self, dataset_id: str) -> None:
        """
        Serialize decisions with publication creation.

        DatasetPublicationStore.publish() uses the same advisory-lock
        key, so approval cannot race with a pipeline decision.
        """

        with self._session.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_advisory_xact_lock(
                    hashtextextended(%s, 0)
                )
                """,
                (f"dataset-publication:{dataset_id}",),
            )
