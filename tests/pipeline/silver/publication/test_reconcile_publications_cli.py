"""Operator-facing contract for the publication reconciliation command."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Self
from unittest.mock import Mock

import pytest

from metrka_core.catalog.publication_projection_models import PublicationProjectionKind
from metrka_core.datasets.workspace_location import WorkspaceLocation
from metrka_core.pipeline.silver import reconcile_publications as command
from metrka_core.pipeline.silver.publication_reconciliation import (
    ProjectionReconciliationResult,
    ProjectionReconciliationStatus,
    SilverPublicationReconciler,
    SilverPublicationReconciliation,
)
from metrka_core.pipeline.silver.reconciliation import (
    PublicationAssetReconciler,
    PublicationEvidenceReconciler,
    PublicationProjectionReconciler,
    PublicationRecordReconciler,
    SilverBuildArtifactReconciler,
)
from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityBatch,
    AssetIntegrityFailureCode,
    AssetIntegrityResult,
    AssetIntegrityStatus,
)
from metrka_core.quality.publication_integrity_models import (
    PublicationIntegrityCheck,
    PublicationIntegrityTrigger,
)

FIXED_NOW = datetime(2026, 8, 20, 9, 30, tzinfo=UTC)
DATASET_ID = "demo.observations"


@dataclass(frozen=True, slots=True)
class _FrozenClock:
    value: datetime = FIXED_NOW

    def now_utc(self) -> datetime:
        return self.value


@dataclass(frozen=True, slots=True)
class _StaticWorkspaceResolver:
    location: WorkspaceLocation

    def resolve(self, workspace_name: str) -> WorkspaceLocation:
        assert workspace_name == "demo"
        return self.location

    def resolve_dataset(self, dataset_id: str) -> WorkspaceLocation:
        assert dataset_id.startswith("demo.")
        return self.location


class _FakePostgresSession:
    opened_conninfos: list[str] = []

    def __init__(self, *, conninfo: str) -> None:
        self.conninfo = conninfo
        self.opened_conninfos.append(conninfo)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


def _projection(projection_kind: PublicationProjectionKind) -> ProjectionReconciliationResult:
    return ProjectionReconciliationResult(
        projection_kind=projection_kind, status=ProjectionReconciliationStatus.SKIPPED
    )


def _report(
    *, asset_verifications: tuple[PublicationIntegrityCheck, ...] = ()
) -> SilverPublicationReconciliation:
    return SilverPublicationReconciliation(
        dataset_id=DATASET_ID,
        current_publication_id=None,
        current_projection=_projection(PublicationProjectionKind.CURRENT),
        history_projection=_projection(PublicationProjectionKind.HISTORY),
        asset_verifications=asset_verifications,
        asset_verification_failures=(),
        manifest_integrity_results=(),
        manifest_integrity_failures=(),
        transformation_detail_integrity_results=(),
        transformation_detail_integrity_failures=(),
        contract_snapshot_integrity_results=(),
        contract_snapshot_integrity_failures=(),
        manifest_failures=(),
        backfilled_publication_ids=(),
        orphans=(),
    )


def _missing_asset_verification() -> PublicationIntegrityCheck:
    result = AssetIntegrityResult(
        file_path="files/silver/tables/demo/missing.parquet",
        status=AssetIntegrityStatus.FAILED,
        expected_size_bytes=128,
        actual_size_bytes=None,
        expected_checksum=f"sha256:{'a' * 64}",
        actual_checksum=None,
        failure_codes=(AssetIntegrityFailureCode.MISSING_FILE,),
    )
    return PublicationIntegrityCheck(
        publication_id="publication-demo",
        trigger=PublicationIntegrityTrigger.RECONCILIATION,
        batch=AssetIntegrityBatch(checked_at=FIXED_NOW, results=(result,)),
    )


def _install_command_runtime(
    monkeypatch: pytest.MonkeyPatch, *, tmp_path: Path, report: SilverPublicationReconciliation
) -> tuple[Mock, dict[str, object]]:
    _FakePostgresSession.opened_conninfos.clear()
    resolver = _StaticWorkspaceResolver(
        WorkspaceLocation.managed(
            workspace_name="demo",
            definition_root=tmp_path / "definitions",
            data_root=tmp_path / "runtime-data",
        )
    )
    reconciler = Mock(spec=SilverPublicationReconciler)
    reconciler.reconcile.return_value = report
    composition: dict[str, object] = {}

    def build_resolver(**_: object) -> _StaticWorkspaceResolver:
        return resolver

    def build_reconciler(**components: object) -> Mock:
        composition.update(components)
        return reconciler

    monkeypatch.setattr(command, "build_workspace_location_resolver", build_resolver)
    monkeypatch.setattr(
        command, "resolve_operations_conninfo", lambda: "postgresql://operator-test"
    )
    monkeypatch.setattr(command, "PostgresSession", _FakePostgresSession)
    monkeypatch.setattr(command, "SilverPublicationReconciler", build_reconciler)
    return reconciler, composition


def test_parser_preserves_the_reconciliation_operator_contract(tmp_path: Path) -> None:
    config_path = tmp_path / "workspaces.yaml"

    arguments = command.build_parser().parse_args(
        [
            "--workspace",
            "demo",
            "--dataset-id",
            "demo.first",
            "--dataset-id",
            "demo.second",
            "--workspaces-config-path",
            str(config_path),
            "--delete-orphans",
            "--grace-hours",
            "12.5",
            "--backfill-publication-assets",
            "--verify-history-assets",
            "--audit-workspace-orphans",
        ]
    )

    assert arguments.workspace == "demo"
    assert arguments.dataset_ids == ["demo.first", "demo.second"]
    assert arguments.workspaces_config_path == config_path
    assert arguments.delete_orphans is True
    assert arguments.grace_hours == 12.5
    assert arguments.backfill_publication_assets is True
    assert arguments.verify_history_assets is True
    assert arguments.audit_workspace_orphans is True


def test_parser_defaults_to_non_deleting_reconciliation() -> None:
    arguments = command.build_parser().parse_args(
        ["--workspace", "demo", "--dataset-id", DATASET_ID]
    )

    assert arguments.delete_orphans is False
    assert arguments.grace_hours == 168.0
    assert arguments.backfill_publication_assets is False
    assert arguments.verify_history_assets is False
    assert arguments.audit_workspace_orphans is False


def test_command_requires_a_dataset_or_workspace_orphan_audit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as captured:
        command.main(["--workspace", "demo"], clock=_FrozenClock())

    assert captured.value.code == 2
    assert "provide --dataset-id or --audit-workspace-orphans" in capsys.readouterr().err


def test_successful_reconciliation_returns_zero_and_builds_focused_components(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    reconciler, composition = _install_command_runtime(
        monkeypatch, tmp_path=tmp_path, report=_report()
    )

    exit_code = command.main(
        ["--workspace", "demo", "--dataset-id", DATASET_ID], clock=_FrozenClock()
    )

    assert exit_code == 0
    assert _FakePostgresSession.opened_conninfos == ["postgresql://operator-test"]
    reconciler.reconcile.assert_called_once_with(
        dataset_id=DATASET_ID,
        delete_orphans=False,
        grace_period=command.timedelta(hours=168.0),
        now=FIXED_NOW,
        verify_history_assets=False,
    )
    assert isinstance(composition["records"], PublicationRecordReconciler)
    assert isinstance(composition["assets"], PublicationAssetReconciler)
    assert isinstance(composition["evidence"], PublicationEvidenceReconciler)
    assert isinstance(composition["projections"], PublicationProjectionReconciler)
    assert isinstance(composition["build_artifacts"], SilverBuildArtifactReconciler)


def test_reconciliation_needs_only_the_dataset_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    reconciler, _ = _install_command_runtime(monkeypatch, tmp_path=tmp_path, report=_report())

    exit_code = command.main(["--dataset-id", DATASET_ID], clock=_FrozenClock())

    assert exit_code == 0
    assert "Workspace: demo" in capsys.readouterr().out
    reconciler.reconcile.assert_called_once()


def test_missing_publication_asset_is_reported_and_returns_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _install_command_runtime(
        monkeypatch,
        tmp_path=tmp_path,
        report=_report(asset_verifications=(_missing_asset_verification(),)),
    )

    exit_code = command.main(
        ["--workspace", "demo", "--dataset-id", DATASET_ID], clock=_FrozenClock()
    )
    output = capsys.readouterr().out

    assert exit_code == 1
    assert "Publication asset integrity failures: 1" in output
    assert "Publication: publication-demo" in output
    assert "File: files/silver/tables/demo/missing.parquet" in output
    assert "Integrity failures: missing_file" in output
    assert f"Expected checksum: sha256:{'a' * 64}" in output
    assert "Actual checksum: unavailable" in output
