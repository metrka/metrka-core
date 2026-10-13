"""PostgreSQL adapter for publication projection health."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from metrka_core.catalog.publication_projection_models import (
    DatasetPublicationProjectionState,
    PublicationProjectionKind,
    PublicationProjectionStatus,
)
from metrka_core.metadata.postgres import PostgresSession, to_jsonb

PROJECTION_STATE_COLUMNS = """
    dataset_id,
    projection_kind,
    expected_publication_id,
    projected_publication_id,
    status,
    status_changed_at,
    last_attempted_at,
    last_synchronized_at,
    error
"""


def _row_to_projection_state(row: Any) -> DatasetPublicationProjectionState:
    record = dict(row)
    raw_error = record["error"]

    if raw_error is not None and not isinstance(raw_error, dict):
        raise TypeError("Publication projection error must be a JSON object")

    projected_publication_id = record["projected_publication_id"]

    return DatasetPublicationProjectionState(
        dataset_id=str(record["dataset_id"]),
        projection_kind=PublicationProjectionKind(str(record["projection_kind"])),
        expected_publication_id=str(record["expected_publication_id"]),
        projected_publication_id=(
            str(projected_publication_id) if projected_publication_id is not None else None
        ),
        status=PublicationProjectionStatus(str(record["status"])),
        status_changed_at=record["status_changed_at"],
        last_attempted_at=record["last_attempted_at"],
        last_synchronized_at=record["last_synchronized_at"],
        error=dict(raw_error) if raw_error is not None else None,
    )


def _require_row(row: Any, *, operation: str) -> DatasetPublicationProjectionState:
    if row is None:
        raise RuntimeError(f"PostgreSQL returned no projection state after {operation}")

    return _row_to_projection_state(row)


class PostgresDatasetPublicationProjectionStateStore:
    """Persist current and history projection synchronization state."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def mark_pending(
        self,
        *,
        dataset_id: str,
        current_publication_id: str,
        history_publication_id: str,
        changed_at: datetime,
    ) -> tuple[DatasetPublicationProjectionState, ...]:
        """Atomically declare both projections pending for a publication."""

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.dataset_publication_projection_states AS projection_state (
                    dataset_id,
                    projection_kind,
                    expected_publication_id,
                    projected_publication_id,
                    status,
                    status_changed_at,
                    last_attempted_at,
                    last_synchronized_at,
                    error
                )
                VALUES (%s, 'current', %s, NULL, 'pending', %s, NULL, NULL, NULL)
                ON CONFLICT (dataset_id, projection_kind)
                DO UPDATE SET
                    expected_publication_id = EXCLUDED.expected_publication_id,
                    status = 'pending',
                    status_changed_at = EXCLUDED.status_changed_at,
                    last_attempted_at = NULL,
                    error = NULL
                WHERE
                    projection_state.expected_publication_id
                    IS DISTINCT FROM EXCLUDED.expected_publication_id
                """,
                (dataset_id, current_publication_id, changed_at),
            )

            cursor.execute(
                """
                INSERT INTO catalog.dataset_publication_projection_states AS projection_state (
                    dataset_id,
                    projection_kind,
                    expected_publication_id,
                    projected_publication_id,
                    status,
                    status_changed_at,
                    last_attempted_at,
                    last_synchronized_at,
                    error
                )
                VALUES (%s, 'history', %s, NULL, 'pending', %s, NULL, NULL, NULL)
                ON CONFLICT (dataset_id, projection_kind)
                DO UPDATE SET
                    expected_publication_id = EXCLUDED.expected_publication_id,
                    status = 'pending',
                    status_changed_at = EXCLUDED.status_changed_at,
                    last_attempted_at = NULL,
                    error = NULL
                """,
                (dataset_id, history_publication_id, changed_at),
            )

            cursor.execute(
                f"""
                SELECT
                    {PROJECTION_STATE_COLUMNS}
                FROM catalog.dataset_publication_projection_states
                WHERE dataset_id = %s
                ORDER BY projection_kind
                """,
                (dataset_id,),
            )

            rows = cursor.fetchall()

        states = tuple(
            sorted(
                (_row_to_projection_state(row) for row in rows),
                key=lambda state: state.projection_kind.value,
            )
        )

        if len(states) != 2:
            raise RuntimeError("PostgreSQL did not return both pending projection states")

        return states

    def mark_synchronized(
        self,
        *,
        dataset_id: str,
        projection_kind: PublicationProjectionKind,
        publication_id: str,
        checked_at: datetime,
    ) -> DatasetPublicationProjectionState:
        """Record that a projection contains the expected publication."""

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO catalog.dataset_publication_projection_states AS projection_state (
                    dataset_id,
                    projection_kind,
                    expected_publication_id,
                    projected_publication_id,
                    status,
                    status_changed_at,
                    last_attempted_at,
                    last_synchronized_at,
                    error
                )
                VALUES (%s, %s, %s, %s, 'synchronized', %s, %s, %s, NULL)
                ON CONFLICT (dataset_id, projection_kind)
                DO UPDATE SET
                    projected_publication_id = EXCLUDED.projected_publication_id,
                    status = 'synchronized',
                    status_changed_at = EXCLUDED.status_changed_at,
                    last_attempted_at = EXCLUDED.last_attempted_at,
                    last_synchronized_at = EXCLUDED.last_synchronized_at,
                    error = NULL
                WHERE
                    projection_state.expected_publication_id
                    = EXCLUDED.expected_publication_id
                RETURNING
                    {PROJECTION_STATE_COLUMNS}
                """,
                (
                    dataset_id,
                    projection_kind.value,
                    publication_id,
                    publication_id,
                    checked_at,
                    checked_at,
                    checked_at,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f"""
                    SELECT
                        {PROJECTION_STATE_COLUMNS}
                    FROM catalog.dataset_publication_projection_states
                    WHERE dataset_id = %s
                      AND projection_kind = %s
                    """,
                    (dataset_id, projection_kind.value),
                )
                row = cursor.fetchone()

        return _require_row(row, operation="mark_synchronized")

    def mark_stale(
        self,
        *,
        dataset_id: str,
        projection_kind: PublicationProjectionKind,
        expected_publication_id: str,
        checked_at: datetime,
        error: Mapping[str, Any],
    ) -> DatasetPublicationProjectionState:
        """Record a failed projection attempt without changing publication truth."""

        structured_error = dict(error)

        if not structured_error:
            raise ValueError("Projection error must not be empty")

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO catalog.dataset_publication_projection_states AS projection_state (
                    dataset_id,
                    projection_kind,
                    expected_publication_id,
                    projected_publication_id,
                    status,
                    status_changed_at,
                    last_attempted_at,
                    last_synchronized_at,
                    error
                )
                VALUES (%s, %s, %s, NULL, 'stale', %s, %s, NULL, %s)
                ON CONFLICT (dataset_id, projection_kind)
                DO UPDATE SET
                    status = 'stale',
                    status_changed_at = EXCLUDED.status_changed_at,
                    last_attempted_at = EXCLUDED.last_attempted_at,
                    error = EXCLUDED.error
                WHERE
                    projection_state.expected_publication_id
                    = EXCLUDED.expected_publication_id
                RETURNING
                    {PROJECTION_STATE_COLUMNS}
                """,
                (
                    dataset_id,
                    projection_kind.value,
                    expected_publication_id,
                    checked_at,
                    checked_at,
                    to_jsonb(structured_error),
                ),
            )

            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f"""
                    SELECT
                        {PROJECTION_STATE_COLUMNS}
                    FROM catalog.dataset_publication_projection_states
                    WHERE dataset_id = %s
                      AND projection_kind = %s
                    """,
                    (dataset_id, projection_kind.value),
                )
                row = cursor.fetchone()

        return _require_row(row, operation="mark_stale")

    def get(
        self, *, dataset_id: str, projection_kind: PublicationProjectionKind
    ) -> DatasetPublicationProjectionState | None:
        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {PROJECTION_STATE_COLUMNS}
                FROM catalog.dataset_publication_projection_states
                WHERE dataset_id = %s
                  AND projection_kind = %s
                """,
                (dataset_id, projection_kind.value),
            )

            row = cursor.fetchone()

        return _row_to_projection_state(row) if row is not None else None
