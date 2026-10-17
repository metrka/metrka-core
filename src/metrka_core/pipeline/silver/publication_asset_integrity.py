"""Verify that immutable publication records still describe their files."""

from __future__ import annotations

from collections.abc import Collection
from datetime import datetime
from typing import Protocol

from metrka_core.pipeline.silver.artifact_ports import SilverArtifactStore
from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityBatch,
    AssetIntegrityFailureCode,
    AssetIntegrityResult,
    AssetIntegrityStatus,
)
from metrka_core.storage.checksums import format_sha256_checksum, parse_sha256_checksum, sha256_file


class PublicationAssetExpectation(Protocol):
    """Minimum publication-asset shape required for integrity verification."""

    @property
    def file_path(self) -> str: ...

    @property
    def size_bytes(self) -> int: ...

    @property
    def checksum(self) -> str: ...


class PublicationAssetIntegrityVerifier(Protocol):
    """Port used by publication and reconciliation workflows."""

    def inspect(
        self, *, assets: Collection[PublicationAssetExpectation], checked_at: datetime
    ) -> AssetIntegrityBatch:
        """Compare expected publication assets with their stored files."""
        ...


class PublicationAssetIntegrityError(RuntimeError):
    """Raised when a candidate publication contains invalid files."""

    def __init__(self, batch: AssetIntegrityBatch) -> None:
        self.batch = batch
        failed_paths = ", ".join(result.file_path for result in batch.failed_results[:3])
        super().__init__(
            "Publication asset integrity verification failed: "
            f"{len(batch.failed_results)} of {len(batch.results)} file(s) failed"
            + (f" ({failed_paths})" if failed_paths else "")
        )


class Sha256PublicationAssetIntegrityVerifier:
    """Compare published files with recorded byte sizes and SHA-256 checksums."""

    def __init__(self, paths: SilverArtifactStore) -> None:
        self._paths = paths

    def inspect(
        self, *, assets: Collection[PublicationAssetExpectation], checked_at: datetime
    ) -> AssetIntegrityBatch:
        if not assets:
            raise ValueError("Publication contains no registered assets")

        return AssetIntegrityBatch(
            checked_at=checked_at, results=tuple(self._inspect_one(asset=asset) for asset in assets)
        )

    def _inspect_one(self, *, asset: PublicationAssetExpectation) -> AssetIntegrityResult:
        try:
            path = self._paths.resolve_publication_asset_path(asset.file_path)
        except ValueError as error:
            return self._failed(
                asset=asset, failure_codes=(AssetIntegrityFailureCode.INVALID_PATH,), error=error
            )

        try:
            actual_size = path.stat().st_size
            actual_digest = sha256_file(path)
            actual_checksum = format_sha256_checksum(actual_digest)
        except FileNotFoundError as error:
            return self._failed(
                asset=asset, failure_codes=(AssetIntegrityFailureCode.MISSING_FILE,), error=error
            )
        except OSError as error:
            return self._failed(
                asset=asset, failure_codes=(AssetIntegrityFailureCode.UNREADABLE_FILE,), error=error
            )

        failure_codes: list[AssetIntegrityFailureCode] = []
        try:
            expected_digest = parse_sha256_checksum(asset.checksum)
        except ValueError:
            expected_digest = None

        if expected_digest is None:
            failure_codes.append(AssetIntegrityFailureCode.INVALID_EXPECTED_CHECKSUM)
        elif expected_digest != actual_digest:
            failure_codes.append(AssetIntegrityFailureCode.CHECKSUM_MISMATCH)

        if asset.size_bytes != actual_size:
            failure_codes.append(AssetIntegrityFailureCode.SIZE_MISMATCH)

        status = AssetIntegrityStatus.FAILED if failure_codes else AssetIntegrityStatus.PASSED

        return AssetIntegrityResult(
            file_path=asset.file_path,
            status=status,
            expected_size_bytes=asset.size_bytes,
            actual_size_bytes=actual_size,
            expected_checksum=asset.checksum,
            actual_checksum=actual_checksum,
            failure_codes=tuple(failure_codes),
        )

    @staticmethod
    def _failed(
        *,
        asset: PublicationAssetExpectation,
        failure_codes: tuple[AssetIntegrityFailureCode, ...],
        error: Exception,
    ) -> AssetIntegrityResult:
        return AssetIntegrityResult(
            file_path=asset.file_path,
            status=AssetIntegrityStatus.FAILED,
            expected_size_bytes=asset.size_bytes,
            actual_size_bytes=None,
            expected_checksum=asset.checksum,
            actual_checksum=None,
            failure_codes=failure_codes,
            error_type=type(error).__name__,
            error_message=str(error),
        )
