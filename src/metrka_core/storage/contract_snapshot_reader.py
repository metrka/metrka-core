"""Local-filesystem adapter for immutable contract snapshot JSON."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from metrka_core.catalog.contract_snapshot_reader import (
    ContractSnapshotFailure,
    ContractSnapshotReader,
    ContractSnapshotReadError,
)
from metrka_core.storage.json_object_reader import JsonObjectReadError, LocalJsonObjectReader


@dataclass(frozen=True, slots=True)
class LocalContractSnapshotReader:
    """Read contract snapshots below one resolved workspace data root."""

    reader: LocalJsonObjectReader

    def read_snapshot(self, *, path: str) -> dict[str, Any]:
        """Read one snapshot without allowing escape from contract storage."""

        try:
            return self.reader.read(path=path)
        except JsonObjectReadError as error:
            raise ContractSnapshotReadError(
                reason=ContractSnapshotFailure(error.reason.value),
                path=error.path,
                message=str(error),
            ) from error


def create_contract_snapshot_reader(*, data_root: Path) -> ContractSnapshotReader:
    """Create a reader scoped to one workspace's contract snapshot storage."""

    if not isinstance(data_root, Path):
        raise TypeError("data_root must be a pathlib.Path")

    return LocalContractSnapshotReader(
        reader=LocalJsonObjectReader(
            data_root=data_root,
            storage_root=data_root / "contracts",
            artifact_label="Contract snapshot",
            storage_label="Contract snapshot storage",
        )
    )


__all__ = ["LocalContractSnapshotReader", "create_contract_snapshot_reader"]
