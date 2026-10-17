"""Reconcile immutable publication evidence with its recorded hashes."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.catalog.publication_manifest_reader import PublicationManifestReader
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.publication_indexes import validate_publication_manifest
from metrka_core.pipeline.silver.reconciliation.models import (
    FileIntegrityExecutionFailure,
    PublicationEvidenceReconciliation,
)
from metrka_core.storage.checksums import format_sha256_checksum
from metrka_core.storage.file_integrity import (
    FileIntegrityExpectation,
    FileIntegrityResult,
    FileIntegrityStatus,
    FileIntegrityVerifier,
)


@dataclass(frozen=True, slots=True)
class PublicationEvidenceReconciler:
    """Verify manifests, transformation details and contract snapshots."""

    silver_builds: PostgresSilverBuildStore
    file_integrity: FileIntegrityVerifier
    transformation_impacts: PostgresTransformationImpactStore
    silver_store: PublicationManifestReader

    def reconcile(
        self, *, publications: tuple[DatasetPublication, ...]
    ) -> PublicationEvidenceReconciliation:
        """Run evidence checks in dependency order and retain partial failures."""

        manifest_results, manifest_failures = self._verify_manifests(publications=publications)
        transformation_results, transformation_failures = self._verify_transformation_details(
            publications=publications
        )
        contract_results, contract_failures = self._verify_contract_snapshots(
            publications=publications, manifest_integrity_results=manifest_results
        )

        return PublicationEvidenceReconciliation(
            manifest_results=manifest_results,
            manifest_failures=manifest_failures,
            transformation_detail_results=transformation_results,
            transformation_detail_failures=transformation_failures,
            contract_snapshot_results=contract_results,
            contract_snapshot_failures=contract_failures,
        )

    def _verify_manifests(
        self, *, publications: tuple[DatasetPublication, ...]
    ) -> tuple[tuple[FileIntegrityResult, ...], tuple[FileIntegrityExecutionFailure, ...]]:
        if not publications:
            return (), ()

        builds = self.silver_builds.find_by_ids(
            tuple(publication.silver_build_id for publication in publications)
        )
        results: list[FileIntegrityResult] = []
        failures: list[FileIntegrityExecutionFailure] = []

        for publication in publications:
            build = builds.get(publication.silver_build_id)

            if build is None:
                failures.append(
                    FileIntegrityExecutionFailure(
                        artifact_kind="silver_manifest",
                        owner_id=publication.publication_id,
                        file_path=publication.manifest_path,
                        error_type="MissingSilverBuild",
                        message=(
                            "Publication references a Silver build that is absent: "
                            f"{publication.silver_build_id}"
                        ),
                    )
                )
                continue

            if build.output_hash is None:
                failures.append(
                    FileIntegrityExecutionFailure(
                        artifact_kind="silver_manifest",
                        owner_id=publication.publication_id,
                        file_path=publication.manifest_path,
                        error_type="MissingManifestHash",
                        message=(
                            "Successful Silver build has no recorded manifest checksum: "
                            f"{publication.silver_build_id}"
                        ),
                    )
                )
                continue

            results.append(
                self.file_integrity.inspect(
                    FileIntegrityExpectation(
                        artifact_kind="silver_manifest",
                        owner_id=publication.publication_id,
                        file_path=publication.manifest_path,
                        expected_checksum=format_sha256_checksum(build.output_hash),
                    )
                )
            )

        return tuple(results), tuple(failures)

    def _verify_transformation_details(
        self, *, publications: tuple[DatasetPublication, ...]
    ) -> tuple[tuple[FileIntegrityResult, ...], tuple[FileIntegrityExecutionFailure, ...]]:
        if not publications:
            return (), ()

        results: list[FileIntegrityResult] = []
        failures: list[FileIntegrityExecutionFailure] = []

        try:
            impacts = self.transformation_impacts.list_for_builds(
                silver_build_ids=tuple(publication.silver_build_id for publication in publications)
            )
        except Exception as error:
            return (), (
                FileIntegrityExecutionFailure(
                    artifact_kind="transformation_details",
                    owner_id=publications[0].dataset_id,
                    file_path="lineage.transformation_impacts",
                    error_type=type(error).__name__,
                    message=str(error),
                ),
            )

        for impact in impacts:
            if impact.details_path is None:
                continue

            if impact.details_hash is None:
                failures.append(
                    FileIntegrityExecutionFailure(
                        artifact_kind="transformation_details",
                        owner_id=impact.transformation_impact_id,
                        file_path=impact.details_path,
                        error_type="MissingTransformationDetailsHash",
                        message="Transformation details file has no recorded checksum",
                    )
                )
                continue

            try:
                result = self.file_integrity.inspect(
                    FileIntegrityExpectation(
                        artifact_kind="transformation_details",
                        owner_id=impact.transformation_impact_id,
                        file_path=impact.details_path,
                        expected_checksum=format_sha256_checksum(impact.details_hash),
                    )
                )
            except Exception as error:
                failures.append(
                    FileIntegrityExecutionFailure(
                        artifact_kind="transformation_details",
                        owner_id=impact.transformation_impact_id,
                        file_path=impact.details_path,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )
                continue

            results.append(result)

        return tuple(results), tuple(failures)

    def _verify_contract_snapshots(
        self,
        *,
        publications: tuple[DatasetPublication, ...],
        manifest_integrity_results: tuple[FileIntegrityResult, ...],
    ) -> tuple[tuple[FileIntegrityResult, ...], tuple[FileIntegrityExecutionFailure, ...]]:
        passed_manifest_ids = {
            result.owner_id
            for result in manifest_integrity_results
            if result.status is FileIntegrityStatus.PASSED
        }
        results: list[FileIntegrityResult] = []
        failures: list[FileIntegrityExecutionFailure] = []

        for publication in publications:
            if publication.publication_id not in passed_manifest_ids:
                failures.append(
                    FileIntegrityExecutionFailure(
                        artifact_kind="contract_snapshot",
                        owner_id=publication.publication_id,
                        file_path=publication.manifest_path,
                        error_type="ManifestIntegrityNotVerified",
                        message=(
                            "Contract snapshot was not trusted because its Silver manifest "
                            "did not pass integrity verification"
                        ),
                    )
                )
                continue

            try:
                manifest = self.silver_store.read_manifest(path=publication.manifest_path)
                validate_publication_manifest(publication=publication, manifest=manifest)
                contract = manifest.get("contract")
                if not isinstance(contract, dict):
                    raise ValueError("Silver manifest contract must be an object")

                snapshot_path = contract.get("snapshot_path")
                checksum = contract.get("checksum")
                if not isinstance(snapshot_path, str) or not snapshot_path.strip():
                    raise ValueError("Silver manifest contract.snapshot_path must be a string")
                if not isinstance(checksum, str) or not checksum.strip():
                    raise ValueError("Silver manifest contract.checksum must be a string")

                result = self.file_integrity.inspect(
                    FileIntegrityExpectation(
                        artifact_kind="contract_snapshot",
                        owner_id=publication.publication_id,
                        file_path=snapshot_path,
                        expected_checksum=checksum,
                    )
                )
            except Exception as error:
                failures.append(
                    FileIntegrityExecutionFailure(
                        artifact_kind="contract_snapshot",
                        owner_id=publication.publication_id,
                        file_path=publication.manifest_path,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )
                continue

            results.append(result)

        return tuple(results), tuple(failures)
