"""Command-line reconciliation for Silver publications."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

from metrka_core.catalog.postgres_publication_asset_store import (
    PostgresDatasetPublicationAssetStore,
)
from metrka_core.catalog.postgres_publication_projection_store import (
    PostgresDatasetPublicationProjectionStateStore,
)
from metrka_core.catalog.postgres_publication_store import PostgresDatasetPublicationStore
from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.operations.database_config import resolve_operations_conninfo
from metrka_core.pipeline.composition.workspace_locations import (
    WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE,
    build_workspace_location_resolver,
)
from metrka_core.pipeline.config import resolve_runtime_environment
from metrka_core.pipeline.runtime_services import Clock, SystemClock
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.publication_asset_integrity import (
    Sha256PublicationAssetIntegrityVerifier,
)
from metrka_core.pipeline.silver.publication_indexes import PublicationBackedSilverIndexService
from metrka_core.pipeline.silver.publication_reconciliation import (
    OrphanCleanupStatus,
    ProjectionReconciliationResult,
    ProjectionReconciliationStatus,
    SilverPublicationReconciler,
)
from metrka_core.pipeline.silver.reconciliation import (
    PublicationAssetReconciler,
    PublicationEvidenceReconciler,
    PublicationProjectionReconciler,
    PublicationRecordReconciler,
    SilverBuildArtifactReconciler,
)
from metrka_core.pipeline.silver.workspace_orphan_audit import (
    SilverWorkspaceOrphanAuditor,
    UnknownArtifactCause,
)
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.storage.file_integrity import Sha256WorkspaceFileIntegrityVerifier
from metrka_core.storage.silver_store import LocalSilverArtifactStore
from metrka_core.storage.workspace_layout import WorkspaceLayout

_UNKNOWN_ARTIFACT_MESSAGES = {
    UnknownArtifactCause.MISSING_BUILD_RECORD: (
        "The directory has a valid Silver build ID, but no matching database "
        "record exists. Check metadata consistency."
    ),
    UnknownArtifactCause.NOT_A_BUILD_ID: (
        "The directory name is not a valid Silver build ID. Inspect the filesystem "
        "for manually created or misplaced files."
    ),
}


def build_parser(*, prog: str | None = None) -> argparse.ArgumentParser:
    """Build the reconciliation CLI parser."""

    parser = argparse.ArgumentParser(
        prog=prog,
        description="Repair Silver publication indexes and report unpublished build artifacts.",
    )

    parser.add_argument(
        "--workspace",
        help=(
            "Optional with --dataset-id, whose datasets already name their workspace; when "
            "given, it must be that same one. Required for --audit-workspace-orphans "
            "without --dataset-id."
        ),
    )

    parser.add_argument(
        "--dataset-id",
        action="append",
        dest="dataset_ids",
        help="Dataset ID to reconcile. Repeat this option to reconcile multiple streams.",
    )

    parser.add_argument(
        "--audit-workspace-orphans",
        action="store_true",
        help=(
            "Scan the workspace once for Silver artifact directories that have no "
            "database build record. Unknown artifacts are reported but never deleted."
        ),
    )

    parser.add_argument(
        "--workspaces-config-path",
        type=Path,
        help=(
            "Path to workspace placement YAML. Defaults to "
            "METRKA_WORKSPACES_CONFIG_PATH or workspaces.local.yaml in development."
        ),
    )

    parser.add_argument(
        "--delete-orphans",
        action="store_true",
        help="Delete eligible failed unpublished builds. Without this flag the command is dry-run.",
    )

    parser.add_argument(
        "--grace-hours",
        type=float,
        default=168.0,
        help="Minimum failed-build age before deletion. Default: 168 hours (7 days).",
    )

    parser.add_argument(
        "--backfill-publication-assets",
        action="store_true",
        help=(
            "Read only manifests whose publications have no registered assets. "
            "Failures are reported without stopping independent projection repairs."
        ),
    )

    parser.add_argument(
        "--verify-history-assets",
        action="store_true",
        help=(
            "Also recompute size and SHA-256 for superseded publication revisions. "
            "Every active publication is always verified."
        ),
    )

    return parser


def reconciliation_location(
    workspace_locations: WorkspaceLocationResolver,
    *,
    dataset_ids: Sequence[str],
    workspace: str | None,
) -> WorkspaceLocation:
    """Find the one folder that holds the datasets to reconcile.

    Each dataset names its own folder, so ``--workspace`` is needed only for an orphan audit
    without datasets; when given alongside datasets it must name the same folder.
    """

    locations = {
        dataset_id: workspace_locations.resolve_dataset(dataset_id) for dataset_id in dataset_ids
    }
    names = sorted({location.name for location in locations.values()})

    if len(names) > 1:
        listing = ", ".join(
            f"{dataset_id} -> {location.name}" for dataset_id, location in locations.items()
        )
        raise RuntimeError(
            f"The datasets live in different workspaces ({listing}); reconcile one "
            "workspace per run."
        )

    if workspace is not None and names and workspace.strip() != names[0]:
        raise RuntimeError(
            f"Datasets {sorted(locations)} belong to workspace {names[0]!r}, not "
            f"{workspace.strip()!r}. Omit --workspace to use theirs."
        )

    if locations:
        return next(iter(locations.values()))

    if workspace is None:
        raise RuntimeError("--audit-workspace-orphans without --dataset-id needs --workspace")

    return workspace_locations.resolve(workspace.strip())


def main(
    argv: Sequence[str] | None = None, *, clock: Clock | None = None, prog: str | None = None
) -> int:
    """Run Silver publication reconciliation."""

    parser = build_parser(prog=prog)
    args = parser.parse_args(argv)

    if not args.dataset_ids and not args.audit_workspace_orphans:
        parser.error("provide --dataset-id or --audit-workspace-orphans")

    resolved_clock = clock if clock is not None else SystemClock()

    if args.grace_hours < 0:
        raise ValueError("--grace-hours must not be negative")

    workspace_locations = build_workspace_location_resolver(
        explicit_config_path=args.workspaces_config_path,
        environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
        runtime_environment=resolve_runtime_environment(os.environ.get("METRKA_ENV")),
    )
    location = reconciliation_location(
        workspace_locations, dataset_ids=args.dataset_ids or (), workspace=args.workspace
    )
    layout = WorkspaceLayout(location=location)

    silver_store = LocalSilverArtifactStore(
        workspace_root=layout.data_root,
        silver_root=layout.silver_dir,
        current_root=layout.current_dir,
    )

    mode = "DELETE" if args.delete_orphans else "DRY RUN"

    print(f"Reconciliation mode: {mode}")
    print(f"Workspace: {location.name}")
    print(f"Grace period: {args.grace_hours:g} hours")

    postgres_conninfo = resolve_operations_conninfo()

    exit_code = 0

    with PostgresSession(conninfo=postgres_conninfo) as postgres_session:
        publications = PostgresDatasetPublicationStore(postgres_session)

        publication_assets = PostgresDatasetPublicationAssetStore(postgres_session)
        projection_states = PostgresDatasetPublicationProjectionStateStore(postgres_session)

        silver_builds = PostgresSilverBuildStore(postgres_session)
        integrity_evidence = PostgresAssetIntegrityEvidenceStore(postgres_session)

        publication_indexes = PublicationBackedSilverIndexService(
            publications=publications,
            publication_assets=publication_assets,
            silver_store=silver_store,
            clock=resolved_clock,
        )

        reconciler = SilverPublicationReconciler(
            records=PublicationRecordReconciler(
                publications=publications,
                publication_assets=publication_assets,
                silver_store=silver_store,
                backfill_publication_assets=args.backfill_publication_assets,
            ),
            assets=PublicationAssetReconciler(
                publication_assets=publication_assets,
                integrity=Sha256PublicationAssetIntegrityVerifier(silver_store),
                integrity_checks=integrity_evidence,
            ),
            evidence=PublicationEvidenceReconciler(
                silver_builds=silver_builds,
                file_integrity=Sha256WorkspaceFileIntegrityVerifier(
                    workspace_root=layout.data_root
                ),
                transformation_impacts=PostgresTransformationImpactStore(postgres_session),
                silver_store=silver_store,
            ),
            projections=PublicationProjectionReconciler(
                publication_indexes=publication_indexes, projection_states=projection_states
            ),
            build_artifacts=SilverBuildArtifactReconciler(
                silver_builds=silver_builds, silver_store=silver_store
            ),
        )

        for dataset_id in args.dataset_ids or ():
            report = reconciler.reconcile(
                dataset_id=dataset_id,
                delete_orphans=args.delete_orphans,
                grace_period=timedelta(hours=args.grace_hours),
                now=resolved_clock.now_utc(),
                verify_history_assets=args.verify_history_assets,
            )

            print()
            print(f"Dataset: {report.dataset_id}")
            print(f"Current publication: {report.current_publication_id or 'none'}")
            _print_projection("Current", report.current_projection)
            _print_projection("History", report.history_projection)
            print(f"Publication assets backfilled: {len(report.backfilled_publication_ids)}")
            verified_asset_count = sum(
                len(check.batch.results) for check in report.asset_verifications
            )
            failed_asset_results = tuple(
                (check.publication_id, asset_result)
                for check in report.asset_verifications
                for asset_result in check.batch.failed_results
            )
            print(f"Publication assets verified: {verified_asset_count}")
            print(f"Publication asset integrity failures: {len(failed_asset_results)}")
            print(
                "Publication verification execution failures: "
                f"{len(report.asset_verification_failures)}"
            )
            failed_manifest_integrity = tuple(
                manifest_result
                for manifest_result in report.manifest_integrity_results
                if manifest_result.failed
            )
            print(f"Silver manifests verified: {len(report.manifest_integrity_results)}")
            print(f"Silver manifest integrity failures: {len(failed_manifest_integrity)}")
            print(
                "Silver manifest verification preparation failures: "
                f"{len(report.manifest_integrity_failures)}"
            )
            failed_transformation_details = tuple(
                transformation_result
                for transformation_result in report.transformation_detail_integrity_results
                if transformation_result.failed
            )
            print(
                "Transformation detail files verified: "
                f"{len(report.transformation_detail_integrity_results)}"
            )
            print(f"Transformation detail integrity failures: {len(failed_transformation_details)}")
            print(
                "Transformation detail verification preparation failures: "
                f"{len(report.transformation_detail_integrity_failures)}"
            )
            failed_contract_snapshots = tuple(
                contract_result
                for contract_result in report.contract_snapshot_integrity_results
                if contract_result.failed
            )
            print(f"Contract snapshots verified: {len(report.contract_snapshot_integrity_results)}")
            print(f"Contract snapshot integrity failures: {len(failed_contract_snapshots)}")
            print(
                "Contract snapshot verification preparation failures: "
                f"{len(report.contract_snapshot_integrity_failures)}"
            )

            for publication_id, asset_result in failed_asset_results:
                print()
                print(f"  Publication: {publication_id}")
                print(f"  File: {asset_result.file_path}")
                print(
                    "  Integrity failures: "
                    + ", ".join(code.value for code in asset_result.failure_codes)
                )
                print(f"  Expected checksum: {asset_result.expected_checksum}")
                print(f"  Actual checksum: {asset_result.actual_checksum or 'unavailable'}")

            for asset_failure in report.asset_verification_failures:
                print()
                print(f"  Publication: {asset_failure.publication_id}")
                print(f"  Verification error: {asset_failure.error_type}: {asset_failure.message}")

            for manifest_result in failed_manifest_integrity:
                print()
                print(f"  Publication: {manifest_result.owner_id}")
                print(f"  Manifest: {manifest_result.file_path}")
                print(
                    "  Integrity failures: "
                    + ", ".join(code.value for code in manifest_result.failure_codes)
                )
                print(f"  Expected checksum: {manifest_result.expected_checksum}")
                print(f"  Actual checksum: {manifest_result.actual_checksum or 'unavailable'}")

            for failure in report.manifest_integrity_failures:
                print()
                print(f"  Publication: {failure.owner_id}")
                print(f"  Manifest: {failure.file_path}")
                print(f"  Verification error: {failure.error_type}: {failure.message}")

            for transformation_result in failed_transformation_details:
                print()
                print(f"  Transformation impact: {transformation_result.owner_id}")
                print(f"  Details file: {transformation_result.file_path}")
                print(
                    "  Integrity failures: "
                    + ", ".join(code.value for code in transformation_result.failure_codes)
                )
                print(f"  Expected checksum: {transformation_result.expected_checksum}")
                print(
                    f"  Actual checksum: {transformation_result.actual_checksum or 'unavailable'}"
                )

            for failure in report.transformation_detail_integrity_failures:
                print()
                print(f"  Transformation impact: {failure.owner_id}")
                print(f"  Details file: {failure.file_path}")
                print(f"  Verification error: {failure.error_type}: {failure.message}")

            for contract_result in failed_contract_snapshots:
                print()
                print(f"  Publication: {contract_result.owner_id}")
                print(f"  Contract snapshot: {contract_result.file_path}")
                print(
                    "  Integrity failures: "
                    + ", ".join(code.value for code in contract_result.failure_codes)
                )
                print(f"  Expected checksum: {contract_result.expected_checksum}")
                print(f"  Actual checksum: {contract_result.actual_checksum or 'unavailable'}")

            for failure in report.contract_snapshot_integrity_failures:
                print()
                print(f"  Publication: {failure.owner_id}")
                print(f"  Manifest: {failure.file_path}")
                print(f"  Verification error: {failure.error_type}: {failure.message}")

            print(f"Historical manifest failures: {len(report.manifest_failures)}")

            for manifest_failure in report.manifest_failures:
                print()
                print(f"  Publication: {manifest_failure.publication_id}")
                print(f"  Manifest: {manifest_failure.manifest_path}")
                print(f"  Error: {manifest_failure.error_type}: {manifest_failure.message}")

            print(f"Orphan builds found: {len(report.orphans)}")

            if (
                report.current_projection.status is ProjectionReconciliationStatus.FAILED
                or report.history_projection.status is ProjectionReconciliationStatus.FAILED
                or failed_asset_results
                or report.asset_verification_failures
                or failed_manifest_integrity
                or report.manifest_integrity_failures
                or failed_transformation_details
                or report.transformation_detail_integrity_failures
                or failed_contract_snapshots
                or report.contract_snapshot_integrity_failures
                or report.manifest_failures
                or any(
                    orphan.cleanup.status is OrphanCleanupStatus.FAILED for orphan in report.orphans
                )
            ):
                exit_code = 1

            for orphan in report.orphans:
                print()
                print(f"  Silver build: {orphan.silver_build_id}")
                print(f"  Status: {orphan.database_status.value}")
                print(f"  Age: {orphan.age_hours:.1f}h")
                print(f"  Eligible for deletion: {orphan.eligible_for_deletion}")
                print(f"  Cleanup status: {orphan.cleanup.status.value}")
                print(f"  Deleted: {orphan.deleted}")
                print(f"  Reason: {orphan.reason}")

                for path in orphan.artifact_directories:
                    print(f"  Path: {path}")

                for error in orphan.cleanup.errors:
                    print(
                        "  Deletion error: "
                        f"{error.artifact_directory}: "
                        f"{error.error_type}: {error.message}"
                    )

        if args.audit_workspace_orphans:
            audit = SilverWorkspaceOrphanAuditor(
                silver_builds=silver_builds, silver_store=silver_store
            ).audit()

            print()
            print("Workspace unknown-artifact audit")
            print(f"Unknown Silver builds found: {len(audit.unknown_builds)}")

            if audit.unknown_builds:
                exit_code = 1

            for unknown in audit.unknown_builds:
                print()
                print(f"  Artifact name: {unknown.artifact_name}")

                if unknown.silver_build_id is not None:
                    print(f"  Silver build: {unknown.silver_build_id}")

                print(f"  Cause: {unknown.cause.value}")
                print(f"  Reason: {_UNKNOWN_ARTIFACT_MESSAGES[unknown.cause]}")

                for path in unknown.artifact_directories:
                    print(f"  Path: {path}")

    return exit_code


def _print_projection(label: str, result: ProjectionReconciliationResult) -> None:
    """Print one projection result without hiding partial recovery."""

    print(f"{label} projection: {result.status.value}")

    for path in result.paths:
        print(f"  Path: {path}")

    if result.error_type is not None:
        print(f"  Error: {result.error_type}: {result.error_message}")


if __name__ == "__main__":
    raise SystemExit(main())
