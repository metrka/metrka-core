"""Models describing one immutable capture of source assets."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, cast

from metrka_core.metadata.artifact import VALID_ARTIFACT_ROLES, ArtifactRole


def _required_text(payload: dict[str, Any], *, field_name: str, location: str) -> str:
    value = payload.get(field_name)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{location}.{field_name} must be a non-empty string")

    return value


def _required_timestamp(payload: dict[str, Any], *, field_name: str, location: str) -> datetime:
    value = _required_text(payload, field_name=field_name, location=location)

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{location}.{field_name} must be an ISO datetime") from exc

    if parsed.utcoffset() is None:
        raise ValueError(f"{location}.{field_name} must be timezone-aware")

    return parsed.astimezone(UTC)


def _optional_timestamp(
    payload: dict[str, Any], *, field_name: str, location: str
) -> datetime | None:
    if payload.get(field_name) is None:
        return None

    return _required_timestamp(payload, field_name=field_name, location=location)


@dataclass(frozen=True)
class SourceCapture:
    """Identity and directory of one source retrieval event."""

    source_capture_id: str
    captured_at: datetime
    directory: Path
    relative_path: str

    def __post_init__(self) -> None:
        if not self.source_capture_id.strip():
            raise ValueError("SourceCapture.source_capture_id must not be empty")

        if self.captured_at.utcoffset() is None:
            raise ValueError("SourceCapture.captured_at must be timezone-aware")

        if not isinstance(self.directory, Path):
            raise TypeError("SourceCapture.directory must be pathlib.Path")

        if not self.relative_path.strip():
            raise ValueError("SourceCapture.relative_path must not be empty")

        if "\\" in self.relative_path:
            raise ValueError("SourceCapture.relative_path must use '/' separators")

        relative_path = PurePosixPath(self.relative_path)

        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("SourceCapture.relative_path must be a safe relative path")


@dataclass(frozen=True)
class SourceCaptureAssetReceipt:
    """One asset listed in a source-capture receipt."""

    stream_name: str
    relative_path: str
    source_url: str
    artifact_role: ArtifactRole
    size_bytes: int
    source_last_modified: datetime | None = None

    def __post_init__(self) -> None:
        for field_name, value in (
            ("stream_name", self.stream_name),
            ("relative_path", self.relative_path),
            ("source_url", self.source_url),
        ):
            if not value.strip():
                raise ValueError(f"SourceCaptureAssetReceipt.{field_name} must not be empty")

        relative_path = PurePosixPath(self.relative_path)

        if relative_path.is_absolute() or ".." in relative_path.parts or "\\" in self.relative_path:
            raise ValueError("SourceCaptureAssetReceipt.relative_path must be a safe POSIX path")

        if self.artifact_role not in VALID_ARTIFACT_ROLES:
            raise ValueError("SourceCaptureAssetReceipt.artifact_role is not supported")

        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int):
            raise TypeError("SourceCaptureAssetReceipt.size_bytes must be an integer")

        if self.size_bytes < 0:
            raise ValueError("SourceCaptureAssetReceipt.size_bytes must not be negative")

        if self.source_last_modified is not None and self.source_last_modified.utcoffset() is None:
            raise ValueError(
                "SourceCaptureAssetReceipt.source_last_modified must be timezone-aware"
            )

    @classmethod
    def from_dict(cls, payload: object, *, index: int) -> SourceCaptureAssetReceipt:
        location = f"source-capture receipt asset {index}"

        if not isinstance(payload, dict):
            raise ValueError(f"{location} must be an object")

        artifact_role_value = _required_text(payload, field_name="artifact_role", location=location)

        if artifact_role_value not in VALID_ARTIFACT_ROLES:
            raise ValueError(f"{location}.artifact_role is not supported")

        size_bytes = payload.get("size_bytes")

        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise ValueError(f"{location}.size_bytes must be an integer")

        return cls(
            stream_name=_required_text(payload, field_name="stream_name", location=location),
            relative_path=_required_text(payload, field_name="relative_path", location=location),
            source_url=_required_text(payload, field_name="source_url", location=location),
            artifact_role=cast(ArtifactRole, artifact_role_value),
            size_bytes=size_bytes,
            source_last_modified=_optional_timestamp(
                payload, field_name="source_last_modified", location=location
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "stream_name": self.stream_name,
            "relative_path": self.relative_path,
            "source_url": self.source_url,
            "artifact_role": self.artifact_role,
            "size_bytes": self.size_bytes,
            "source_last_modified": (
                self.source_last_modified.isoformat()
                if self.source_last_modified is not None
                else None
            ),
        }


@dataclass(frozen=True)
class SourceCaptureReceipt:
    """Immutable receipt for one completed source capture."""

    source_capture_id: str
    pipeline_run_id: str
    captured_at: datetime
    assets: tuple[SourceCaptureAssetReceipt, ...]

    def __post_init__(self) -> None:
        if not self.source_capture_id.strip():
            raise ValueError("SourceCaptureReceipt.source_capture_id must not be empty")

        if not self.pipeline_run_id.strip():
            raise ValueError("SourceCaptureReceipt.pipeline_run_id must not be empty")

        if self.captured_at.utcoffset() is None:
            raise ValueError("SourceCaptureReceipt.captured_at must be timezone-aware")

        asset_identities = [(asset.stream_name, asset.relative_path) for asset in self.assets]

        if len(asset_identities) != len(set(asset_identities)):
            raise ValueError("SourceCaptureReceipt.assets must not repeat stream and relative path")

    @classmethod
    def from_dict(cls, payload: object) -> SourceCaptureReceipt:
        location = "source-capture receipt"

        if not isinstance(payload, dict):
            raise ValueError(f"{location} must be an object")

        schema_version = payload.get("schema_version")

        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != 1
        ):
            raise ValueError(f"{location}.schema_version must be 1")

        raw_assets = payload.get("assets")

        if not isinstance(raw_assets, list):
            raise ValueError(f"{location}.assets must be a list")

        asset_count = payload.get("asset_count")

        if isinstance(asset_count, bool) or not isinstance(asset_count, int):
            raise ValueError(f"{location}.asset_count must be an integer")

        if asset_count < 0:
            raise ValueError(f"{location}.asset_count must not be negative")

        if asset_count != len(raw_assets):
            raise ValueError(f"{location}.asset_count does not match the assets list")

        return cls(
            source_capture_id=_required_text(
                payload, field_name="source_capture_id", location=location
            ),
            pipeline_run_id=_required_text(
                payload, field_name="pipeline_run_id", location=location
            ),
            captured_at=_required_timestamp(payload, field_name="captured_at", location=location),
            assets=tuple(
                SourceCaptureAssetReceipt.from_dict(raw_asset, index=index)
                for index, raw_asset in enumerate(raw_assets)
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "source_capture_id": self.source_capture_id,
            "pipeline_run_id": self.pipeline_run_id,
            "captured_at": self.captured_at.isoformat(),
            "asset_count": len(self.assets),
            "assets": [asset.to_dict() for asset in self.assets],
        }


@dataclass(frozen=True)
class SourceCaptureAssetBinding:
    """Bind one captured asset to its File Marshal identity."""

    stream_name: str
    dataset_id: str
    dataset_file_id: str
    relative_path: str
    source_url: str
    artifact_role: ArtifactRole
    source_last_modified: datetime | None = None

    def __post_init__(self) -> None:
        for field_name, value in (
            ("stream_name", self.stream_name),
            ("dataset_id", self.dataset_id),
            ("dataset_file_id", self.dataset_file_id),
            ("relative_path", self.relative_path),
            ("source_url", self.source_url),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")

        relative_path = PurePosixPath(self.relative_path)

        if relative_path.is_absolute() or ".." in relative_path.parts or "\\" in self.relative_path:
            raise ValueError("relative_path must be a safe POSIX path")

        if self.source_last_modified is not None and self.source_last_modified.utcoffset() is None:
            raise ValueError("source_last_modified must be timezone-aware")
