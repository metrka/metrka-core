"""PostgreSQL publication of an approved Silver candidate."""

from __future__ import annotations

from metrka_core.catalog.postgres_publication_candidate_store import (
    PostgresDatasetPublicationCandidateStore,
)
from metrka_core.catalog.publication_asset_store import DatasetPublicationAssetStore
from metrka_core.catalog.publication_candidate_models import (
    DatasetPublicationCandidate,
    DatasetPublicationCandidateStatus,
)
from metrka_core.catalog.publication_manifest_reader import PublicationManifestReader
from metrka_core.catalog.publication_models import DatasetPublicationRequest
from metrka_core.catalog.publication_projection_store import DatasetPublicationProjectionStateStore
from metrka_core.catalog.publication_store import DatasetPublicationStore
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.runtime_services import PublicationIdGenerator
from metrka_core.pipeline.silver.approved_publication_unit_of_work import (
    ApprovedPublicationCommand,
    ApprovedPublicationResult,
)
from metrka_core.pipeline.silver.build_models import SilverBuild, SilverBuildStatus
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.publication_asset_integrity import (
    PublicationAssetIntegrityError,
    PublicationAssetIntegrityVerifier,
)
from metrka_core.pipeline.silver.publication_asset_mapping import publication_assets_from_manifest
from metrka_core.pipeline.silver.publication_indexes import validate_publication_manifest
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.quality.postgres_publication_gate_evidence_store import (
    PostgresPublicationGateEvidenceStore,
)
from metrka_core.quality.publication_gate_evidence_models import PublicationGateAttempt
from metrka_core.quality.publication_integrity_models import (
    PublicationIntegrityBatchLink,
    PublicationIntegrityTrigger,
)


class PostgresApprovedPublicationUnitOfWork:
    """Atomically publish an approved Silver build."""

    def __init__(
        self,
        *,
        session: PostgresSession,
        candidates: PostgresDatasetPublicationCandidateStore,
        silver_builds: PostgresSilverBuildStore,
        publications: DatasetPublicationStore,
        publication_assets: DatasetPublicationAssetStore,
        publication_asset_integrity: PublicationAssetIntegrityVerifier,
        asset_integrity_batches: PostgresAssetIntegrityEvidenceStore,
        publication_integrity: PostgresAssetIntegrityEvidenceStore,
        publication_gate_evidence: PostgresPublicationGateEvidenceStore,
        projection_states: DatasetPublicationProjectionStateStore,
        silver_store: PublicationManifestReader,
        publication_ids: PublicationIdGenerator,
    ) -> None:
        self._session = session
        self._candidates = candidates
        self._silver_builds = silver_builds
        self._publications = publications
        self._publication_assets = publication_assets
        self._publication_asset_integrity = publication_asset_integrity
        self._asset_integrity_batches = asset_integrity_batches
        self._publication_integrity = publication_integrity
        self._publication_gate_evidence = publication_gate_evidence
        self._projection_states = projection_states
        self._silver_store = silver_store
        self._publication_ids = publication_ids

    def commit(self, command: ApprovedPublicationCommand) -> ApprovedPublicationResult:
        integrity_error: PublicationAssetIntegrityError | None = None
        result: ApprovedPublicationResult | None = None

        with self._session.transaction():
            candidate = self._candidates.get_by_id_for_update(command.candidate_id)

            if candidate is None:
                raise KeyError(f"Unknown publication candidate: {command.candidate_id}")

            if candidate.status is DatasetPublicationCandidateStatus.PUBLISHED:
                return self._existing_result(candidate)

            if candidate.status is not DatasetPublicationCandidateStatus.APPROVED:
                raise RuntimeError(
                    "Publication candidate must be approved "
                    "before publishing. Candidate "
                    f"{candidate.candidate_id} has status "
                    f"{candidate.status.value}."
                )

            self._lock_dataset(candidate.dataset_id)

            build = self._silver_builds.get_by_id(candidate.silver_build_id)

            if build is None:
                raise RuntimeError(
                    "Publication candidate references an "
                    "unknown Silver build: "
                    f"{candidate.silver_build_id}"
                )

            manifest_path = self._validate_candidate_build(candidate=candidate, build=build)

            self._validate_baseline(candidate)

            manifest = self._silver_store.read_manifest(path=manifest_path)

            requested_assets = publication_assets_from_manifest(manifest)

            asset_verification = self._publication_asset_integrity.inspect(
                assets=requested_assets, checked_at=command.published_at
            )
            integrity_batch_id = self._asset_integrity_batches.insert_batch(asset_verification)
            self._publication_gate_evidence.insert_attempt(
                PublicationGateAttempt(
                    candidate_id=candidate.candidate_id,
                    silver_build_id=candidate.silver_build_id,
                    pipeline_run_id=build.pipeline_run_id,
                    integrity_batch_id=integrity_batch_id,
                )
            )

            if not asset_verification.passed:
                integrity_error = PublicationAssetIntegrityError(asset_verification)
            else:
                publication_id = self._publication_ids.new_publication_id()
                publication = self._publications.publish(
                    DatasetPublicationRequest(
                        publication_id=publication_id,
                        pipeline_run_id=build.pipeline_run_id,
                        dataset_id=candidate.dataset_id,
                        version_period=candidate.version_period,
                        partition_key=candidate.partition_key,
                        partition_value=candidate.partition_value,
                        silver_build_id=candidate.silver_build_id,
                        engine_release_id=build.engine_release_id,
                        processing_config_hash=build.processing_config_hash,
                        quality_config_hash=build.quality_config_hash,
                        fingerprint_version=candidate.fingerprint_version,
                        logical_hash_algorithm=candidate.logical_hash_algorithm,
                        schema_hash_algorithm=candidate.schema_hash_algorithm,
                        logical_data_hash=candidate.logical_data_hash,
                        schema_hash=candidate.schema_hash,
                        manifest_path=manifest_path,
                        published_at=command.published_at,
                    )
                )

                if publication.publication_id != publication_id:
                    raise RuntimeError(
                        "Publication store returned a different publication_id: "
                        f"expected={publication_id}, actual={publication.publication_id}"
                    )

                validate_publication_manifest(publication=publication, manifest=manifest)

                registered_assets = self._publication_assets.register(
                    publication_id=publication.publication_id, assets=requested_assets
                )

                self._publication_integrity.link_batch(
                    PublicationIntegrityBatchLink(
                        publication_id=publication.publication_id,
                        trigger=PublicationIntegrityTrigger.PUBLICATION_COMMIT,
                        integrity_batch_id=integrity_batch_id,
                    )
                )

                published_candidate = self._candidates.mark_published(
                    candidate_id=candidate.candidate_id, publication_id=publication.publication_id
                )

                current_publication = self._publications.find_current(publication.dataset_id)

                if current_publication is None:
                    raise RuntimeError(
                        "Publishing a candidate produced no current dataset publication: "
                        f"{publication.dataset_id}"
                    )

                self._projection_states.mark_pending(
                    dataset_id=publication.dataset_id,
                    current_publication_id=current_publication.publication_id,
                    history_publication_id=publication.publication_id,
                    changed_at=command.published_at,
                )

                result = ApprovedPublicationResult(
                    candidate=published_candidate,
                    publication=publication,
                    current_publication=current_publication,
                    publication_assets=registered_assets,
                )

        if integrity_error is not None:
            raise integrity_error

        if result is None:
            raise RuntimeError("Publication transaction completed without a result")

        return result

    def _existing_result(self, candidate: DatasetPublicationCandidate) -> ApprovedPublicationResult:
        publication_id = candidate.publication_id

        if publication_id is None:
            raise RuntimeError("Published candidate contains no publication_id")

        publication = self._publications.get_by_id(publication_id)

        if publication is None:
            raise RuntimeError(
                f"Published candidate references an unknown publication: {publication_id}"
            )

        assets = self._publication_assets.list_for_publication(publication_id=publication_id)

        if not assets:
            raise RuntimeError(
                f"Published candidate has no registered assets: {candidate.candidate_id}"
            )

        current_publication = self._publications.find_current(publication.dataset_id)

        if current_publication is None:
            raise RuntimeError(
                "Published candidate belongs to a dataset without a current publication: "
                f"{publication.dataset_id}"
            )

        return ApprovedPublicationResult(
            candidate=candidate,
            publication=publication,
            current_publication=current_publication,
            publication_assets=assets,
        )

    def _validate_baseline(self, candidate: DatasetPublicationCandidate) -> None:
        active = self._publications.find_active(
            dataset_id=candidate.dataset_id, partition_value=candidate.partition_value
        )

        actual_baseline_id = active.publication_id if active is not None else None

        if actual_baseline_id != candidate.baseline_publication_id:
            raise RuntimeError(
                "Publication candidate is stale. Its "
                "approved baseline is no longer the active "
                "publication. Rebuild the dataset and review "
                "a new candidate."
            )

    @staticmethod
    def _validate_candidate_build(
        *, candidate: DatasetPublicationCandidate, build: SilverBuild
    ) -> str:
        if build.status is not SilverBuildStatus.SUCCEEDED:
            raise RuntimeError(
                f"Only a successful Silver build can be published: {build.silver_build_id}"
            )

        expected_values = {
            "silver_build_id": (candidate.silver_build_id, build.silver_build_id),
            "dataset_id": (candidate.dataset_id, build.dataset_id),
            "version_period": (candidate.version_period, build.version_period),
            "partition_key": (candidate.partition_key, build.partition_key),
            "partition_value": (candidate.partition_value, build.partition_value),
            "fingerprint_version": (candidate.fingerprint_version, build.fingerprint_version),
            "logical_hash_algorithm": (
                candidate.logical_hash_algorithm,
                build.logical_hash_algorithm,
            ),
            "schema_hash_algorithm": (candidate.schema_hash_algorithm, build.schema_hash_algorithm),
            "logical_data_hash": (candidate.logical_data_hash, build.logical_data_hash),
            "schema_hash": (candidate.schema_hash, build.schema_hash),
        }

        for field_name, (candidate_value, build_value) in expected_values.items():
            if candidate_value != build_value:
                raise RuntimeError(
                    f"Publication candidate does not match its Silver build field {field_name!r}"
                )

        manifest_path = build.manifest_path

        if manifest_path is None or not manifest_path.strip():
            raise RuntimeError(
                f"Successful Silver build contains no manifest path: {build.silver_build_id}"
            )

        return manifest_path

    def _lock_dataset(self, dataset_id: str) -> None:
        with self._session.cursor() as cursor:
            cursor.execute(
                """
                SELECT pg_advisory_xact_lock(
                    hashtextextended(%s, 0)
                )
                """,
                (f"dataset-publication:{dataset_id}",),
            )
