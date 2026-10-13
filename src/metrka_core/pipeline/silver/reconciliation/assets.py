"""Reconcile registered publication assets with immutable files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from metrka_core.catalog.publication_asset_store import DatasetPublicationAssetStore
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.pipeline.silver.publication_asset_integrity import (
    PublicationAssetIntegrityVerifier,
)
from metrka_core.pipeline.silver.reconciliation.models import (
    AssetVerificationFailure,
    PublicationAssetReconciliation,
)
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.quality.publication_integrity_models import (
    PublicationIntegrityCheck,
    PublicationIntegrityTrigger,
)


@dataclass(frozen=True, slots=True)
class PublicationAssetReconciler:
    """Verify publication files and persist every verification outcome."""

    publication_assets: DatasetPublicationAssetStore
    integrity: PublicationAssetIntegrityVerifier
    integrity_checks: PostgresAssetIntegrityEvidenceStore

    def reconcile(
        self, *, publications: tuple[DatasetPublication, ...], checked_at: datetime
    ) -> PublicationAssetReconciliation:
        """Verify each selected publication without blocking independent repairs."""

        checks: list[PublicationIntegrityCheck] = []
        failures: list[AssetVerificationFailure] = []

        for publication in publications:
            try:
                assets = self.publication_assets.list_for_publication(
                    publication_id=publication.publication_id
                )
                batch = self.integrity.inspect(assets=assets, checked_at=checked_at)
                check = PublicationIntegrityCheck(
                    publication_id=publication.publication_id,
                    trigger=PublicationIntegrityTrigger.RECONCILIATION,
                    batch=batch,
                )
                self.integrity_checks.insert_check(check)
            except Exception as error:
                failures.append(
                    AssetVerificationFailure(
                        publication_id=publication.publication_id,
                        error_type=type(error).__name__,
                        message=str(error),
                    )
                )
                continue

            checks.append(check)

        return PublicationAssetReconciliation(verifications=tuple(checks), failures=tuple(failures))
