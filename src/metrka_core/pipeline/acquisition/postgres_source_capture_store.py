"""PostgreSQL persistence for source captures."""

from __future__ import annotations

from typing import Any

from metrka_core.datasets.dataset_identity import WorkspaceName
from metrka_core.metadata.postgres import PostgresSession
from metrka_core.pipeline.acquisition.models import SourceCapture, SourceCaptureAssetBinding


def _capture_identity(row: Any) -> tuple[object, ...]:
    record = dict(row)

    return (
        str(record["source_capture_id"]),
        str(record["workspace_name"]),
        record["captured_at"],
        str(record["capture_path"]),
    )


def _asset_identity(row: Any) -> tuple[object, ...]:
    record = dict(row)

    return (
        str(record["stream_name"]),
        str(record["dataset_id"]),
        str(record["dataset_file_id"]),
        str(record["relative_path"]),
        str(record["source_url"]),
        str(record["artifact_role"]),
        record["source_last_modified"],
    )


class PostgresSourceCaptureStore:
    """Persist captures and their File Marshal bindings."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def register_capture(
        self, *, capture: SourceCapture, pipeline_run_id: str, workspace_name: WorkspaceName
    ) -> None:
        """
        Register one immutable capture.

        The current pipeline run is also bound to the capture in the same
        PostgreSQL transaction.
        """

        normalized_pipeline_run_id = pipeline_run_id.strip()
        normalized_workspace_name = workspace_name.strip()

        if not normalized_pipeline_run_id:
            raise ValueError("pipeline_run_id must not be empty")

        if not normalized_workspace_name:
            raise ValueError("workspace_name must not be empty")

        expected_identity = (
            capture.source_capture_id,
            normalized_workspace_name,
            capture.captured_at,
            capture.relative_path,
        )

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meta.source_captures (
                    source_capture_id,
                    workspace_name,
                    captured_at,
                    capture_path
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_capture_id)
                DO NOTHING
                """,
                expected_identity,
            )

            cursor.execute(
                """
                SELECT
                    source_capture_id,
                    workspace_name,
                    captured_at,
                    capture_path
                FROM meta.source_captures
                WHERE source_capture_id = %s
                FOR UPDATE
                """,
                (capture.source_capture_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(f"Source capture was not persisted: {capture.source_capture_id}")

            if _capture_identity(row) != expected_identity:
                raise RuntimeError(
                    "Source capture ID already exists with "
                    "different immutable metadata: "
                    f"{capture.source_capture_id}"
                )

            cursor.execute(
                """
                UPDATE logs.pipeline_runs
                SET source_capture_id = %s
                WHERE pipeline_run_id = %s
                  AND (
                        source_capture_id IS NULL
                        OR source_capture_id = %s
                  )
                """,
                (capture.source_capture_id, normalized_pipeline_run_id, capture.source_capture_id),
            )

            if cursor.rowcount != 1:
                cursor.execute(
                    """
                    SELECT source_capture_id
                    FROM logs.pipeline_runs
                    WHERE pipeline_run_id = %s
                    """,
                    (normalized_pipeline_run_id,),
                )

                pipeline_row = cursor.fetchone()

                if pipeline_row is None:
                    raise RuntimeError(
                        "Pipeline run was not found while "
                        "registering source capture: "
                        f"{normalized_pipeline_run_id}"
                    )

                existing_capture_id = dict(pipeline_row).get("source_capture_id")

                raise RuntimeError(
                    "Pipeline run is already bound to a "
                    "different source capture: "
                    f"pipeline_run_id="
                    f"{normalized_pipeline_run_id}, "
                    f"source_capture_id="
                    f"{existing_capture_id}"
                )

    def bind_assets(
        self, *, source_capture_id: str, assets: tuple[SourceCaptureAssetBinding, ...]
    ) -> None:
        """
        Bind captured assets to their File Marshal identities.

        The operation is idempotent. Repeating the same binding succeeds,
        while conflicting immutable metadata raises an error.
        """

        normalized_capture_id = source_capture_id.strip()

        if not normalized_capture_id:
            raise ValueError("source_capture_id must not be empty")

        if not assets:
            return

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM meta.source_captures
                WHERE source_capture_id = %s
                """,
                (normalized_capture_id,),
            )

            if cursor.fetchone() is None:
                raise RuntimeError(
                    f"Cannot bind assets to an unknown source capture: {normalized_capture_id}"
                )

            for asset in assets:
                expected_identity = (
                    asset.stream_name,
                    asset.dataset_id,
                    asset.dataset_file_id,
                    asset.relative_path,
                    asset.source_url,
                    asset.artifact_role,
                    asset.source_last_modified,
                )

                cursor.execute(
                    """
                    INSERT INTO meta.source_capture_assets (
                        source_capture_id,
                        stream_name,
                        dataset_id,
                        dataset_file_id,
                        relative_path,
                        source_url,
                        artifact_role,
                        source_last_modified
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (
                        source_capture_id,
                        stream_name,
                        relative_path
                    )
                    DO NOTHING
                    """,
                    (normalized_capture_id, *expected_identity),
                )

                cursor.execute(
                    """
                    SELECT
                        stream_name,
                        dataset_id,
                        dataset_file_id,
                        relative_path,
                        source_url,
                        artifact_role,
                        source_last_modified
                    FROM meta.source_capture_assets
                      WHERE source_capture_id = %s
                      AND stream_name = %s
                      AND relative_path = %s
                    """,
                    (normalized_capture_id, asset.stream_name, asset.relative_path),
                )

                row = cursor.fetchone()

                if row is None:
                    raise RuntimeError(
                        "Source capture asset was not "
                        "persisted: "
                        f"{normalized_capture_id}/"
                        f"{asset.stream_name}/"
                        f"{asset.relative_path}"
                    )

                if _asset_identity(row) != expected_identity:
                    raise RuntimeError(
                        "Source capture asset already exists "
                        "with different immutable metadata: "
                        f"{normalized_capture_id}/"
                        f"{asset.stream_name}/"
                        f"{asset.relative_path}"
                    )
