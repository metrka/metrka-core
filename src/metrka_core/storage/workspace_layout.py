"""Deterministic filesystem layout for one logical workspace.

Definition paths are derived from ``definition_root``. Mutable artifacts are
derived independently from ``data_root``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from metrka_core.datasets.workspace_location import WorkspaceLocation


@dataclass(frozen=True, slots=True)
class WorkspaceLayout:
    """Derive definition and persistent data paths for one workspace."""

    location: WorkspaceLocation

    def __post_init__(self) -> None:
        if not isinstance(self.location, WorkspaceLocation):
            raise TypeError("location must be a WorkspaceLocation")

    @property
    def definition_root(self) -> Path:
        return self.location.definition_root

    @property
    def data_root(self) -> Path:
        return self.location.data_root

    def _require_non_empty(self, value: str, name: str) -> str:
        """Validate that a required string argument is not empty."""
        if not value:
            raise ValueError(f"{name} is required")
        return value

    # ---------------------------------------------------------------------------------------------
    # Top-level directories
    # ---------------------------------------------------------------------------------------------
    @property
    def conf_dir(self) -> Path:
        """<definition_root>/conf"""

        return self.definition_root / "conf"

    @property
    def logs_dir(self) -> Path:
        """<data_root>/logs"""

        return self.data_root / "logs"

    # ---------------------------------------------------------------------------------------------
    # Files
    # ---------------------------------------------------------------------------------------------

    @property
    def files_dir(self) -> Path:
        """<data_root>/files"""
        return self.data_root / "files"

    @property
    def bronze_dir(self) -> Path:
        """<data_root>/files/bronze"""
        return self.files_dir / "bronze"

    @property
    def bronze_landing_dir(self) -> Path:
        """<data_root>/files/bronze/landing"""
        return self.bronze_dir / "landing"

    @property
    def bronze_runs_dir(self) -> Path:
        """<data_root>/files/bronze/runs"""
        return self.bronze_dir / "runs"

    @property
    def silver_dir(self) -> Path:
        """<data_root>/files/silver"""
        return self.files_dir / "silver"

    @property
    def silver_tables_dir(self) -> Path:
        """<data_root>/files/silver/tables"""
        return self.silver_dir / "tables"

    @property
    def silver_manifests_dir(self) -> Path:
        """<data_root>/files/silver/manifests"""
        return self.silver_dir / "manifests"

    @property
    def silver_views_dir(self) -> Path:
        """<data_root>/files/silver/views"""
        return self.silver_dir / "views"

    @property
    def silver_transformation_impacts_dir(self) -> Path:
        """
        <data_root>/files/silver/transformation_impacts
        """

        return self.silver_dir / "transformation_impacts"

    def bronze_run_dir(self, run_id: str) -> Path:
        """<data_root>/files/bronze/runs/<run_id>"""
        run_id = self._require_non_empty(run_id, "run_id")
        return self.bronze_runs_dir / run_id

    # ---------------------------------------------------------------------------------------------
    # Receipts (execution summary)
    # ---------------------------------------------------------------------------------------------
    @property
    def receipts_dir(self) -> Path:
        """<data_root>/receipts"""
        return self.data_root / "receipts"

    @property
    def executions_dir(self) -> Path:
        """<data_root>/receipts/executions"""
        return self.receipts_dir / "executions"

    # ---------------------------------------------------------------------------------------------
    # Dataset contracts snapshots
    # ---------------------------------------------------------------------------------------------

    @property
    def contract_snapshots_dir(self) -> Path:
        """<data_root>/contracts"""
        return self.data_root / "contracts"

    # ---------------------------------------------------------------------------------------------
    # Current dataset truth (pointers / checks / latest state)
    # ---------------------------------------------------------------------------------------------

    @property
    def current_dir(self) -> Path:
        """<data_root>/current"""
        return self.data_root / "current"

    @property
    def current_latest_dir(self) -> Path:
        """<data_root>/current/latest"""
        return self.current_dir / "latest"

    @property
    def current_checks_dir(self) -> Path:
        """<data_root>/current/checks"""
        return self.current_dir / "checks"

    @property
    def bronze_latest_dir(self) -> Path:
        """<data_root>/current/latest/bronze"""
        return self.current_latest_dir / "bronze"

    @property
    def silver_latest_dir(self) -> Path:
        """<data_root>/current/latest/silver"""
        return self.current_latest_dir / "silver"

    @property
    def bronze_execution_marker_path(self) -> Path:
        """<data_root>/current/latest/bronze/_run_in_progress.json"""
        return self.bronze_latest_dir / "_run_in_progress.json"

    @property
    def silver_execution_marker_path(self) -> Path:
        """<data_root>/current/latest/silver/_run_in_progress.json"""
        return self.silver_latest_dir / "_run_in_progress.json"

    # ---------------------------------------------------------------------------------------------
    # Config
    # ---------------------------------------------------------------------------------------------

    def config_path(self, name: str = "config.yaml") -> Path:
        """<definition_root>/conf/<name>"""
        name = self._require_non_empty(name, "name")
        return self.conf_dir / name
