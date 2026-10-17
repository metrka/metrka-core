"""Domain failures raised by the FileMarshal lifecycle service."""

from __future__ import annotations


class DuplicateSourceFileError(ValueError):
    """A source hash is already registered for the same dataset."""

    def __init__(self, *, dataset_id: str, source_hash: str) -> None:
        self.dataset_id = dataset_id
        self.source_hash = source_hash
        super().__init__(
            f"Source file is already registered for dataset_id={dataset_id}: "
            f"sha256={source_hash[:8]}"
        )
