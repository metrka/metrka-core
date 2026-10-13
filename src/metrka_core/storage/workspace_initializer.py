"""Initialization of local workspace directories."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from metrka_core.storage.workspace_layout import WorkspaceLayout


@dataclass(frozen=True)
class LocalWorkspaceInitializer:
    """Create the standard dataset structure on a local filesystem."""

    layout: WorkspaceLayout

    def required_directories(self) -> tuple[Path, ...]:
        """Return all directories required by a workspace."""

        return (
            self.layout.logs_dir,
            self.layout.data_root,
            self.layout.files_dir,
            self.layout.bronze_dir,
            self.layout.bronze_landing_dir,
            self.layout.bronze_runs_dir,
            self.layout.silver_dir,
            self.layout.silver_tables_dir,
            self.layout.silver_manifests_dir,
            self.layout.silver_views_dir,
            self.layout.silver_transformation_impacts_dir,
            self.layout.current_dir,
            self.layout.current_latest_dir,
            self.layout.current_checks_dir,
            self.layout.bronze_latest_dir,
            self.layout.silver_latest_dir,
            self.layout.receipts_dir,
            self.layout.executions_dir,
            self.layout.contract_snapshots_dir,
        )

    def ensure_structure(self) -> None:
        """Create all required directories safely and idempotently."""

        for directory in self.required_directories():
            directory.mkdir(parents=True, exist_ok=True)
