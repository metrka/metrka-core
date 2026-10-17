"""Storage boundary for active pipeline configuration files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from metrka_core.storage.path_segments import require_path_segment


class ConfigStore(Protocol):
    """Resolve active pipeline configuration files."""

    def path(self, *, name: str) -> Path:
        """Resolve one configuration file."""
        ...


@dataclass(frozen=True)
class LocalConfigStore:
    """Resolve configuration files on a local filesystem."""

    workspace_root: Path
    config_root: Path

    def __post_init__(self) -> None:
        for field_name in ("workspace_root", "config_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            object.__setattr__(self, field_name, value.expanduser().resolve())

        try:
            self.config_root.relative_to(self.workspace_root)
        except ValueError as error:
            raise ValueError("config_root must be inside workspace_root") from error

    def path(self, *, name: str) -> Path:
        """Resolve one active configuration file."""

        return self.config_root / require_path_segment(name, "config name")
