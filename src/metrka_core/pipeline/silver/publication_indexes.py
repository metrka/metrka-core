"""Publication-backed derived indexes for Silver datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from metrka_core.catalog.publication_asset_store import DatasetPublicationAssetStore
from metrka_core.catalog.publication_models import DatasetPublication
from metrka_core.catalog.publication_store import DatasetPublicationStore
from metrka_core.pipeline.runtime_services import Clock
from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.pipeline.silver.silver_artifacts import (
    write_silver_history_views,
    write_silver_latest_views,
)


@dataclass(frozen=True)
class SilverPublicationIndexResult:
    """Derived indexes generated from publication records."""

    current_publication: DatasetPublication
    pointer_path: Path
    view_paths: tuple[Path, ...]


class SilverPublicationIndexService(Protocol):
    """Maintain recoverable Silver publication indexes."""

    def refresh_current(self, *, dataset_id: str) -> SilverPublicationIndexResult:
        """Refresh projections required for the current publication."""
        ...

    def rebuild_history(self, *, dataset_id: str) -> tuple[Path, ...]:
        """Regenerate history views from active publications."""
        ...


class PublicationBackedSilverIndexService:
    """Build Silver indexes from authoritative publications."""

    def __init__(
        self,
        *,
        publications: DatasetPublicationStore,
        publication_assets: DatasetPublicationAssetStore,
        silver_store: SilverArtifactStore,
        clock: Clock,
    ) -> None:
        self._publications = publications
        self._publication_assets = publication_assets
        self._silver_store = silver_store
        self._clock = clock

    def refresh_current(self, *, dataset_id: str) -> SilverPublicationIndexResult:
        """Regenerate pointer and views from PostgreSQL state."""

        current = self._publications.find_current(dataset_id)

        if current is None:
            raise RuntimeError(
                "Cannot refresh Silver publication indexes "
                f"without a current publication: {dataset_id}"
            )

        current_manifest = self._silver_store.read_manifest(path=current.manifest_path)

        self._validate_manifest(publication=current, manifest=current_manifest)

        view_paths = write_silver_latest_views(
            silver_store=self._silver_store,
            current_manifest=current_manifest,
            publication_id=current.publication_id,
        )

        pointer_payload = {
            "schema_version": 1,
            "dataset_id": current.dataset_id,
            "layer": "silver",
            "publication_id": (current.publication_id),
            "publication_revision": (current.revision),
            "published_at_utc": (current.published_at.isoformat()),
            "pipeline_run_id": (current.pipeline_run_id),
            "silver_build_id": (current.silver_build_id),
            "bronze_run_id": (_required_manifest_string(current_manifest, "bronze_run_id")),
            "silver_run_id": (_required_manifest_string(current_manifest, "silver_run_id")),
            "version_period": (current.version_period.isoformat()),
            "version_period_grain": (
                _required_manifest_string(current_manifest, "version_period_grain")
            ),
            "version_period_source": (
                _required_manifest_string(current_manifest, "version_period_source")
            ),
            "partition_key": (current.partition_key),
            "partition_value": (current.partition_value),
            "manifest_path": (current.manifest_path),
            "view_paths": [self._silver_store.relative_path(path) for path in view_paths],
            "updated_at_utc": (self._clock.now_utc().isoformat()),
        }

        pointer_path = self._silver_store.write_latest_pointer(
            dataset_id=dataset_id, payload=pointer_payload
        )

        return SilverPublicationIndexResult(
            current_publication=current, pointer_path=pointer_path, view_paths=tuple(view_paths)
        )

    def rebuild_history(self, *, dataset_id: str) -> tuple[Path, ...]:
        """Regenerate history views from publication assets."""

        assets = self._publication_assets.list_active(dataset_id=dataset_id)

        if not assets:
            return ()

        return tuple(
            write_silver_history_views(
                silver_store=self._silver_store,
                dataset_id=dataset_id,
                history_entries=[asset.to_view_entry() for asset in assets],
            )
        )

    @staticmethod
    def _validate_manifest(*, publication: DatasetPublication, manifest: dict[str, Any]) -> None:
        """Verify that a manifest belongs to its publication."""

        if manifest.get("dataset_id") != publication.dataset_id:
            raise ValueError(
                f"Publication manifest dataset_id mismatch for {publication.publication_id}"
            )

        if str(manifest.get("silver_build_id")) != publication.silver_build_id:
            raise ValueError(
                f"Publication manifest silver_build_id mismatch for {publication.publication_id}"
            )

        if manifest.get("version_period") != publication.version_period.isoformat():
            raise ValueError(
                f"Publication manifest version_period mismatch for {publication.publication_id}"
            )

        if manifest.get("partition_key") != publication.partition_key:
            raise ValueError(
                f"Publication manifest partition_key mismatch for {publication.publication_id}"
            )

        if manifest.get("partition_value") != publication.partition_value:
            raise ValueError(
                f"Publication manifest partition_value mismatch for {publication.publication_id}"
            )

        fingerprints = manifest.get("fingerprints")
        if not isinstance(fingerprints, dict):
            raise ValueError(
                f"Publication manifest contains no fingerprint identity: "
                f"{publication.publication_id}"
            )

        expected_fingerprint_fields: dict[str, object] = {
            "fingerprint_version": publication.fingerprint_version,
            "logical_data_algorithm": publication.logical_hash_algorithm,
            "schema_algorithm": publication.schema_hash_algorithm,
            "logical_data_hash": publication.logical_data_hash,
            "schema_hash": publication.schema_hash,
        }

        for field_name, expected_value in expected_fingerprint_fields.items():
            if fingerprints.get(field_name) != expected_value:
                raise ValueError(
                    f"Publication manifest fingerprint {field_name} mismatch for "
                    f"{publication.publication_id}"
                )

        if not isinstance(manifest.get("tables"), list):
            raise ValueError(
                f"Publication manifest contains no table list: {publication.publication_id}"
            )


def validate_publication_manifest(
    *, publication: DatasetPublication, manifest: dict[str, Any]
) -> None:
    """Validate that a manifest belongs to its publication."""

    PublicationBackedSilverIndexService._validate_manifest(
        publication=publication, manifest=manifest
    )


def _required_manifest_string(manifest: dict[str, Any], field_name: str) -> str:
    """Read one required non-empty manifest string."""

    value = manifest.get(field_name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Silver manifest field {field_name!r} must be a non-empty string")

    return value.strip()
