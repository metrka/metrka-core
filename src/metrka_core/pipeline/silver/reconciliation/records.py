"""Reconcile publication records with their immutable manifests."""

from __future__ import annotations

from dataclasses import dataclass

from metrka_core.catalog.publication_asset_store import DatasetPublicationAssetStore
from metrka_core.catalog.publication_manifest_reader import PublicationManifestReader
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_store import DatasetPublicationStore
from metrka_core.pipeline.silver.publication_asset_mapping import publication_assets_from_manifest
from metrka_core.pipeline.silver.publication_indexes import validate_publication_manifest
from metrka_core.pipeline.silver.reconciliation.models import (
    ManifestReconciliationFailure,
    PublicationRecordReconciliation,
)


@dataclass(frozen=True, slots=True)
class PublicationRecordReconciler:
    """Select publication records and optionally restore their asset rows."""

    publications: DatasetPublicationStore
    publication_assets: DatasetPublicationAssetStore
    silver_store: PublicationManifestReader
    backfill_publication_assets: bool = False

    def reconcile(
        self, *, dataset_id: str, include_superseded_history: bool
    ) -> PublicationRecordReconciliation:
        """Load one dataset's publication scope before independent checks run."""

        current_publication = self.publications.find_current(dataset_id)
        all_publications = tuple(self.publications.list_all(dataset_id=dataset_id))
        active_publications = tuple(
            publication for publication in all_publications if publication.is_active_revision
        )
        integrity_publications = self._integrity_publications(
            current_publication=current_publication,
            active_publications=active_publications,
            all_publications=all_publications,
            include_superseded_history=include_superseded_history,
        )

        backfilled_publication_ids: tuple[str, ...] = ()
        manifest_failures: tuple[ManifestReconciliationFailure, ...] = ()

        if self.backfill_publication_assets:
            backfilled_publication_ids, manifest_failures = self._backfill_missing_assets(
                dataset_id=dataset_id
            )

        return PublicationRecordReconciliation(
            dataset_id=dataset_id,
            current_publication=current_publication,
            all_publications=all_publications,
            integrity_publications=integrity_publications,
            manifest_failures=manifest_failures,
            backfilled_publication_ids=backfilled_publication_ids,
        )

    @staticmethod
    def _integrity_publications(
        *,
        current_publication: DatasetPublication | None,
        active_publications: tuple[DatasetPublication, ...],
        all_publications: tuple[DatasetPublication, ...],
        include_superseded_history: bool,
    ) -> tuple[DatasetPublication, ...]:
        publications_by_id = {
            publication.publication_id: publication for publication in active_publications
        }

        if current_publication is not None:
            publications_by_id.setdefault(current_publication.publication_id, current_publication)

        if include_superseded_history:
            for publication in all_publications:
                publications_by_id.setdefault(publication.publication_id, publication)

        return tuple(publications_by_id.values())

    def _backfill_missing_assets(
        self, *, dataset_id: str
    ) -> tuple[tuple[str, ...], tuple[ManifestReconciliationFailure, ...]]:
        backfilled_publication_ids: list[str] = []
        failures: list[ManifestReconciliationFailure] = []

        for publication in self.publications.list_active(dataset_id=dataset_id):
            try:
                existing_assets = self.publication_assets.list_for_publication(
                    publication_id=publication.publication_id
                )

                if existing_assets:
                    continue

                manifest = self.silver_store.read_manifest(path=publication.manifest_path)
                validate_publication_manifest(publication=publication, manifest=manifest)
                self.publication_assets.register(
                    publication_id=publication.publication_id,
                    assets=publication_assets_from_manifest(manifest),
                )
            except Exception as error:
                failures.append(
                    ManifestReconciliationFailure(
                        publication_id=publication.publication_id,
                        manifest_path=publication.manifest_path,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )
                continue

            backfilled_publication_ids.append(publication.publication_id)

        return tuple(backfilled_publication_ids), tuple(failures)
