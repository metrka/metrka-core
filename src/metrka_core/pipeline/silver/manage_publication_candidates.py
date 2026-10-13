"""Administrative CLI for Silver publication candidates."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path

from metrka_core.catalog.postgres_publication_asset_store import (
    PostgresDatasetPublicationAssetStore,
)
from metrka_core.catalog.postgres_publication_candidate_store import (
    PostgresDatasetPublicationCandidateStore,
)
from metrka_core.catalog.postgres_publication_projection_store import (
    PostgresDatasetPublicationProjectionStateStore,
)
from metrka_core.catalog.postgres_publication_store import PostgresDatasetPublicationStore
from metrka_core.datasets.path_resolver import WorkspaceLocationResolver
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.operations.database_config import resolve_operations_conninfo
from metrka_core.pipeline.composition.workspace_locations import (
    WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE,
    build_workspace_location_resolver,
)
from metrka_core.pipeline.config import resolve_runtime_environment
from metrka_core.pipeline.runtime_services import Clock, SystemClock, UuidPublicationIdGenerator
from metrka_core.pipeline.silver.approved_publication_unit_of_work import ApprovedPublicationCommand
from metrka_core.pipeline.silver.postgres_approved_publication_unit_of_work import (
    PostgresApprovedPublicationUnitOfWork,
)
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.pipeline.silver.publication_asset_integrity import (
    Sha256PublicationAssetIntegrityVerifier,
)
from metrka_core.pipeline.silver.publication_indexes import PublicationBackedSilverIndexService
from metrka_core.pipeline.silver.publication_projection import (
    refresh_current_publication_projection,
    refresh_history_publication_projection,
)
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.quality.postgres_publication_gate_evidence_store import (
    PostgresPublicationGateEvidenceStore,
)
from metrka_core.storage.silver_store import LocalSilverArtifactStore
from metrka_core.storage.workspace_layout import WorkspaceLayout


def _build_parser(*, prog: str | None = None) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog, description="Inspect, approve, and reject Silver publication candidates."
    )

    commands = parser.add_subparsers(dest="command", required=True)

    list_command = commands.add_parser("list")
    list_command.add_argument("--dataset-id")

    approve = commands.add_parser("approve")
    approve.add_argument("candidate_id")
    approve.add_argument("--approved-by", required=True)

    reject = commands.add_parser("reject")
    reject.add_argument("candidate_id")
    reject.add_argument("--rejected-by", required=True)
    reject.add_argument("--reason", required=True)

    publish = commands.add_parser("publish")
    publish.add_argument("candidate_id")
    publish.add_argument(
        "--workspace",
        help=(
            "Optional. The candidate's dataset already names its workspace; when given, "
            "this must be the same one, for example wi_dhs_adult_lead."
        ),
    )
    publish.add_argument(
        "--workspaces-config-path",
        type=Path,
        help=(
            "Path to workspace placement YAML. Defaults to "
            "METRKA_WORKSPACES_CONFIG_PATH or workspaces.local.yaml in development."
        ),
    )

    return parser


def candidate_location(
    workspace_locations: WorkspaceLocationResolver, *, dataset_id: str, workspace: str | None
) -> WorkspaceLocation:
    """Find the folder of a candidate's dataset; an explicit workspace must name the same one."""

    location = workspace_locations.resolve_dataset(dataset_id)

    if workspace is not None and workspace.strip() != location.name:
        raise RuntimeError(
            f"Candidate dataset {dataset_id!r} belongs to workspace {location.name!r}, "
            f"not {workspace.strip()!r}. Omit --workspace to use the candidate's own."
        )

    return location


def main(
    argv: Sequence[str] | None = None, *, clock: Clock | None = None, prog: str | None = None
) -> int:
    """Run the Silver publication-candidate administration command.

    ``argv`` supplies command arguments without the executable name. ``clock``
    supports deterministic governance timestamps, and ``prog`` overrides the
    name shown in help. Successful list, approval, rejection, and publication
    commands return zero; parser, configuration, database, and governance
    failures propagate as exceptions or ``argparse`` exits.
    """

    args = _build_parser(prog=prog).parse_args(argv)

    resolved_clock = clock if clock is not None else SystemClock()

    with PostgresSession(resolve_operations_conninfo()) as session:
        store = PostgresDatasetPublicationCandidateStore(session)

        if args.command == "list":
            candidates = store.list_awaiting_approval(dataset_id=args.dataset_id)

            if not candidates:
                print("No publication candidates are awaiting approval.")
                return 0

            for candidate in candidates:
                print(
                    candidate.candidate_id,
                    candidate.dataset_id,
                    candidate.partition_value,
                    candidate.change_kind.value,
                    candidate.status.value,
                    candidate.baseline_publication_id or "initial",
                    candidate.requested_at.isoformat(),
                )

            return 0

        if args.command == "approve":
            candidate = store.approve(
                candidate_id=args.candidate_id,
                approved_by=args.approved_by,
                approved_at=resolved_clock.now_utc(),
            )

            print(
                "Approved:", candidate.candidate_id, candidate.dataset_id, candidate.partition_value
            )
            print("The candidate is approved but has not been published.")

            return 0

        if args.command == "reject":
            candidate = store.reject(
                candidate_id=args.candidate_id,
                rejected_by=args.rejected_by,
                rejection_reason=args.reason,
                rejected_at=resolved_clock.now_utc(),
            )

            print(
                "Rejected:", candidate.candidate_id, candidate.dataset_id, candidate.partition_value
            )

            return 0

        if args.command != "publish":
            raise RuntimeError(f"Unsupported command: {args.command}")

        publication_candidate = store.get_by_id(args.candidate_id)

        if publication_candidate is None:
            raise RuntimeError(f"Publication candidate does not exist: {args.candidate_id}")

        workspace_locations = build_workspace_location_resolver(
            explicit_config_path=args.workspaces_config_path,
            environment_config_path=os.environ.get(WORKSPACES_CONFIG_ENVIRONMENT_VARIABLE),
            runtime_environment=resolve_runtime_environment(os.environ.get("METRKA_ENV")),
        )
        layout = WorkspaceLayout(
            location=candidate_location(
                workspace_locations,
                dataset_id=publication_candidate.dataset_id,
                workspace=args.workspace,
            )
        )

        silver_store = LocalSilverArtifactStore(
            workspace_root=layout.data_root,
            silver_root=layout.silver_dir,
            current_root=layout.current_dir,
        )

        publications = PostgresDatasetPublicationStore(session)

        publication_assets = PostgresDatasetPublicationAssetStore(session)
        projection_states = PostgresDatasetPublicationProjectionStateStore(session)
        publication_ids = UuidPublicationIdGenerator()
        integrity_evidence = PostgresAssetIntegrityEvidenceStore(session)

        publisher = PostgresApprovedPublicationUnitOfWork(
            session=session,
            candidates=store,
            silver_builds=PostgresSilverBuildStore(session),
            publications=publications,
            publication_assets=publication_assets,
            publication_asset_integrity=Sha256PublicationAssetIntegrityVerifier(silver_store),
            asset_integrity_batches=integrity_evidence,
            publication_integrity=integrity_evidence,
            publication_gate_evidence=PostgresPublicationGateEvidenceStore(session),
            projection_states=projection_states,
            silver_store=silver_store,
            publication_ids=publication_ids,
        )

        result = publisher.commit(
            ApprovedPublicationCommand(
                candidate_id=args.candidate_id, published_at=resolved_clock.now_utc()
            )
        )

        print(
            "Published:",
            result.publication.publication_id,
            result.publication.dataset_id,
            result.publication.partition_value,
            f"revision={result.publication.revision}",
        )

        indexes = PublicationBackedSilverIndexService(
            publications=publications,
            publication_assets=publication_assets,
            silver_store=silver_store,
            clock=resolved_clock,
        )

        projection_warnings = 0

        try:
            refresh_current_publication_projection(
                dataset_id=result.publication.dataset_id,
                publication=result.current_publication,
                checked_at=resolved_clock.now_utc(),
                publication_indexes=indexes,
                projection_states=projection_states,
            )
        except Exception as error:
            projection_warnings += 1
            print(
                "WARNING: publication committed, but the current pointer/view refresh failed:",
                f"{type(error).__name__}: {error}",
            )

        try:
            refresh_history_publication_projection(
                dataset_id=result.publication.dataset_id,
                expected_publication_id=result.publication.publication_id,
                checked_at=resolved_clock.now_utc(),
                publication_indexes=indexes,
                projection_states=projection_states,
            )
        except Exception as error:
            projection_warnings += 1
            print(
                "WARNING: publication committed, but the history-view refresh failed:",
                f"{type(error).__name__}: {error}",
            )

        if projection_warnings:
            print("Run reconcile_publications to repair the derived projections.")
        else:
            print("Current and history projections were refreshed.")

        return 0


if __name__ == "__main__":
    raise SystemExit(main())
