"""Architectural contract for focused publication reconciliation."""

from __future__ import annotations

from dataclasses import fields

from metrka_core.pipeline.silver.publication_reconciliation import SilverPublicationReconciler
from metrka_core.pipeline.silver.reconciliation import (
    PublicationAssetReconciler,
    PublicationEvidenceReconciler,
    PublicationProjectionReconciler,
    PublicationRecordReconciler,
    SilverBuildArtifactReconciler,
)


def _field_names(value: type[object]) -> tuple[str, ...]:
    return tuple(field.name for field in fields(value))


def test_publication_reconciler_depends_only_on_focused_reconcilers() -> None:
    assert _field_names(SilverPublicationReconciler) == (
        "records",
        "assets",
        "evidence",
        "projections",
        "build_artifacts",
    )


def test_each_reconciler_owns_only_its_subject_dependencies() -> None:
    assert _field_names(PublicationRecordReconciler) == (
        "publications",
        "publication_assets",
        "silver_store",
        "backfill_publication_assets",
    )
    assert _field_names(PublicationAssetReconciler) == (
        "publication_assets",
        "integrity",
        "integrity_checks",
    )
    assert _field_names(PublicationEvidenceReconciler) == (
        "silver_builds",
        "file_integrity",
        "transformation_impacts",
        "silver_store",
    )
    assert _field_names(PublicationProjectionReconciler) == (
        "publication_indexes",
        "projection_states",
    )
    assert _field_names(SilverBuildArtifactReconciler) == ("silver_builds", "silver_store")
