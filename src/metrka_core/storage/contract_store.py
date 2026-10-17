"""Storage boundary for immutable contract snapshots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from metrka_core.catalog.glossary import Glossary, published_contract_bytes
from metrka_core.storage.atomic_writes import atomic_write_bytes, atomic_write_text
from metrka_core.storage.checksums import parse_sha256_hex, sha256_file
from metrka_core.storage.path_segments import require_path_segment


@dataclass(frozen=True)
class ContractSnapshotRef:
    """Logical identity of one contract snapshot."""

    dataset_id: str
    sha256_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "dataset_id", require_path_segment(self.dataset_id, "dataset_id"))

        if not isinstance(self.sha256_hash, str):
            raise TypeError("sha256_hash must be a string")

        normalized_hash = self.sha256_hash.strip().lower()

        try:
            parse_sha256_hex(normalized_hash)
        except ValueError as error:
            raise ValueError(
                "sha256_hash must be a 64-character hexadecimal SHA-256 hash"
            ) from error

        object.__setattr__(self, "sha256_hash", normalized_hash)


@dataclass(frozen=True)
class ContractSnapshotPaths:
    """Physical files belonging to one contract snapshot."""

    yaml_path: Path
    json_path: Path


class ContractSnapshotStore(Protocol):
    """Storage operations required by contract snapshots."""

    def snapshot_paths(self, *, snapshot: ContractSnapshotRef) -> ContractSnapshotPaths:
        """Resolve the files belonging to a snapshot."""
        ...

    def published_bytes(self, contract_path: Path) -> bytes:
        """Return a contract as its snapshot keeps it, with the definitions it uses."""
        ...

    def write_snapshot(
        self, *, snapshot: ContractSnapshotRef, yaml_bytes: bytes, json_payload: Any
    ) -> ContractSnapshotPaths:
        """Persist an immutable contract snapshot."""
        ...

    def definition_relative_path(self, path: str | Path) -> str:
        """Return an active contract path relative to ``definition_root``."""
        ...

    def snapshot_relative_path(self, path: str | Path) -> str:
        """Return an immutable snapshot path relative to ``data_root``."""
        ...


@dataclass(frozen=True)
class LocalContractSnapshotStore:
    """Store contract snapshots on a local filesystem."""

    definition_root: Path
    data_root: Path
    snapshots_root: Path
    glossary: Glossary = field(default_factory=Glossary)

    def __post_init__(self) -> None:
        for field_name in ("definition_root", "data_root", "snapshots_root"):
            value = getattr(self, field_name)

            if not isinstance(value, Path):
                raise TypeError(f"{field_name} must be a pathlib.Path")

            object.__setattr__(self, field_name, value.expanduser().resolve())

        try:
            self.snapshots_root.relative_to(self.data_root)
        except ValueError as error:
            raise ValueError("snapshots_root must be inside data_root") from error

    def published_bytes(self, contract_path: Path) -> bytes:
        """Return a contract as its snapshot keeps it, with the definitions it uses."""

        return published_contract_bytes(contract_path, self.glossary)

    def snapshot_paths(self, *, snapshot: ContractSnapshotRef) -> ContractSnapshotPaths:
        """Resolve content-addressed snapshot paths."""

        snapshot_dir = self.snapshots_root / snapshot.dataset_id / f"sha256={snapshot.sha256_hash}"

        return ContractSnapshotPaths(
            yaml_path=snapshot_dir / "contract.yaml", json_path=snapshot_dir / "contract.json"
        )

    def write_snapshot(
        self, *, snapshot: ContractSnapshotRef, yaml_bytes: bytes, json_payload: Any
    ) -> ContractSnapshotPaths:
        """Persist an immutable YAML and JSON snapshot."""

        if not isinstance(yaml_bytes, bytes):
            raise TypeError("yaml_bytes must be bytes")

        actual_hash = hashlib.sha256(yaml_bytes).hexdigest()

        if actual_hash != snapshot.sha256_hash:
            raise ValueError(
                "Contract content does not match "
                f"snapshot hash: expected "
                f"{snapshot.sha256_hash}, "
                f"found {actual_hash}"
            )

        paths = self.snapshot_paths(snapshot=snapshot)

        if not paths.yaml_path.exists():
            atomic_write_bytes(paths.yaml_path, yaml_bytes)

        persisted_hash = sha256_file(paths.yaml_path)

        if persisted_hash != snapshot.sha256_hash:
            raise ValueError(
                "Persisted contract snapshot checksum "
                f"mismatch: expected "
                f"{snapshot.sha256_hash}, "
                f"found {persisted_hash}"
            )

        if not paths.json_path.exists():
            atomic_write_text(
                paths.json_path, json.dumps(json_payload, indent=2, ensure_ascii=False)
            )

        return paths

    def definition_relative_path(self, path: str | Path) -> str:
        """Return an active contract path relative to ``definition_root``."""

        resolved_path = Path(path).expanduser().resolve()
        try:
            definition_relative_path = resolved_path.relative_to(self.definition_root)
        except ValueError as error:
            raise ValueError(
                f"Contract definition path is outside definition_root: {resolved_path}"
            ) from error

        return definition_relative_path.as_posix()

    def snapshot_relative_path(self, path: str | Path) -> str:
        """Return an immutable snapshot path relative to ``data_root``."""

        resolved_path = Path(path).expanduser().resolve()
        try:
            snapshot_relative_path = resolved_path.relative_to(self.data_root)
        except ValueError as error:
            raise ValueError(
                f"Contract snapshot path is outside data_root: {resolved_path}"
            ) from error

        return snapshot_relative_path.as_posix()
