"""Acquire configured files from direct HTTP URLs."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.pipeline.acquisition.dependencies import AcquisitionDeps
from metrka_core.pipeline.acquisition.models import SourceCapture
from metrka_core.pipeline.action_runtime import ActionRuntime
from metrka_core.pipeline.models import LandedAsset
from metrka_core.storage.atomic_writes import atomic_write

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 1024 * 1024


def _parse_http_last_modified(value: str | None) -> datetime | None:
    """Parse an HTTP Last-Modified header as a UTC datetime."""

    if value is None or not value.strip():
        return None

    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        logger.warning("Could not parse HTTP Last-Modified header: %r", value)
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC)


def _download_http_file(
    *, source_url: str, destination: Path, timeout_seconds: float, min_bytes: int, user_agent: str
) -> datetime | None:
    """Download one HTTP file atomically into landing."""

    request = Request(source_url, headers={"User-Agent": user_agent})
    source_last_modified: datetime | None = None

    def download(temporary_path: Path) -> None:
        nonlocal source_last_modified

        with (
            urlopen(request, timeout=timeout_seconds) as response,
            temporary_path.open("wb") as output,
        ):
            source_last_modified = _parse_http_last_modified(response.headers.get("Last-Modified"))

            while chunk := response.read(_CHUNK_SIZE):
                output.write(chunk)

        downloaded_size = temporary_path.stat().st_size

        if downloaded_size < min_bytes:
            raise RuntimeError(
                f"Downloaded file is too small: {downloaded_size} bytes from {source_url}"
            )

    atomic_write(destination, download)
    return source_last_modified


def extract_http_files(
    runtime: ActionRuntime, deps: AcquisitionDeps, capture: SourceCapture, options: dict[str, Any]
) -> list[LandedAsset]:
    """Download direct HTTP files configured for dataset streams."""

    allowed_options = {"timeout_seconds", "min_bytes", "user_agent"}
    unexpected_options = set(options) - allowed_options

    if unexpected_options:
        raise RuntimeError(f"http.files received unsupported options: {sorted(unexpected_options)}")

    timeout_seconds = options.get("timeout_seconds", 120)
    min_bytes = options.get("min_bytes", 1)
    user_agent = options.get("user_agent", "Metrka data pipeline")

    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or timeout_seconds <= 0
    ):
        raise RuntimeError("http.files timeout_seconds must be a positive number")

    if isinstance(min_bytes, bool) or not isinstance(min_bytes, int) or min_bytes < 1:
        raise RuntimeError("http.files min_bytes must be a positive integer")

    if not isinstance(user_agent, str) or not user_agent.strip():
        raise RuntimeError("http.files user_agent must be a non-empty string")

    landed_assets: list[LandedAsset] = []

    with run_step(
        dataset=runtime.dataset_name,
        step="http_file_extraction",
        layer="landing",
        execution_log_store=deps.execution_log_store,
        start_meta=ExecutionStepMeta(
            source_capture_id=capture.source_capture_id,
            extra={"streams": list(deps.source_config.streams)},
        ),
    ) as step_context:
        for stream_name, stream in deps.source_config.streams.items():
            try:
                download_url = stream.extra.get("download_url")

                if not isinstance(download_url, str) or not download_url.strip():
                    raise RuntimeError(
                        f"Stream {stream_name!r} must define a non-empty download_url"
                    )

                landing_path = deps.landing_store.allocate_path(capture, stream.official_filename)
                source_last_modified = _download_http_file(
                    source_url=download_url,
                    destination=landing_path,
                    timeout_seconds=float(timeout_seconds),
                    min_bytes=min_bytes,
                    user_agent=user_agent.strip(),
                )

                landed_assets.append(
                    LandedAsset(
                        source_capture_id=capture.source_capture_id,
                        stream_name=stream_name,
                        path=landing_path,
                        source_url=download_url,
                        artifact_role=stream.artifact_role,
                        source_last_modified=source_last_modified,
                    )
                )

                step_context.count_success()

                logger.info(
                    "Landed HTTP asset for stream %s: %s (source_last_modified=%s)",
                    stream_name,
                    landing_path,
                    (
                        source_last_modified.isoformat()
                        if source_last_modified is not None
                        else "not provided"
                    ),
                )
            except Exception:
                step_context.count_failed()

                logger.exception("HTTP acquisition failed for stream %s", stream_name)
                raise

        step_context.set_finish_meta(
            ExecutionStepMeta(
                source_capture_id=capture.source_capture_id,
                extra={"assets_landed": len(landed_assets)},
            )
        )
    return landed_assets
