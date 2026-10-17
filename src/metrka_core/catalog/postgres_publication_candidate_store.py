"""PostgreSQL persistence for publication candidates."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from metrka_core.catalog.publication_candidate_models import (
    DatasetPublicationCandidate,
    DatasetPublicationCandidateRequest,
    DatasetPublicationCandidateStatus,
    SilverPublicationChangeKind,
)
from metrka_core.metadata.postgres import PostgresSession

CANDIDATE_COLUMNS = """
    candidate_id,
    dataset_id,
    version_period,
    partition_key,
    partition_value,
    silver_build_id,
    baseline_publication_id,
    change_kind,
    status,
    fingerprint_version,
    logical_hash_algorithm,
    schema_hash_algorithm,
    logical_data_hash,
    schema_hash,
    requested_at,
    approved_at,
    approved_by,
    rejected_at,
    rejected_by,
    rejection_reason,
    publication_id
"""


def _require_non_empty(field_name: str, value: str) -> str:
    normalized = value.strip()

    if not normalized:
        raise ValueError(f"{field_name} must not be empty")

    return normalized


def _require_aware_datetime(field_name: str, value: datetime) -> None:
    if value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _optional_string(value: object) -> str | None:
    if value is None:
        return None

    return str(value)


def _row_to_candidate(row: Any) -> DatasetPublicationCandidate:
    record = dict(row)

    return DatasetPublicationCandidate(
        candidate_id=str(record["candidate_id"]),
        dataset_id=str(record["dataset_id"]),
        version_period=record["version_period"],
        partition_key=str(record["partition_key"]),
        partition_value=str(record["partition_value"]),
        silver_build_id=str(record["silver_build_id"]),
        baseline_publication_id=_optional_string(record["baseline_publication_id"]),
        change_kind=SilverPublicationChangeKind(str(record["change_kind"])),
        status=DatasetPublicationCandidateStatus(str(record["status"])),
        fingerprint_version=int(record["fingerprint_version"]),
        logical_hash_algorithm=str(record["logical_hash_algorithm"]),
        schema_hash_algorithm=str(record["schema_hash_algorithm"]),
        logical_data_hash=str(record["logical_data_hash"]),
        schema_hash=str(record["schema_hash"]),
        requested_at=record["requested_at"],
        approved_at=record["approved_at"],
        approved_by=_optional_string(record["approved_by"]),
        rejected_at=record["rejected_at"],
        rejected_by=_optional_string(record["rejected_by"]),
        rejection_reason=_optional_string(record["rejection_reason"]),
        publication_id=_optional_string(record["publication_id"]),
    )


def _matches_request(
    *, candidate: DatasetPublicationCandidate, request: DatasetPublicationCandidateRequest
) -> bool:
    return (
        candidate.dataset_id == request.dataset_id
        and candidate.version_period == request.version_period
        and candidate.partition_key == request.partition_key
        and candidate.partition_value == request.partition_value
        and candidate.silver_build_id == request.silver_build_id
        and (candidate.baseline_publication_id == request.baseline_publication_id)
        and candidate.change_kind is request.change_kind
        and (candidate.fingerprint_version == request.fingerprint_version)
        and candidate.logical_hash_algorithm == request.logical_hash_algorithm
        and candidate.schema_hash_algorithm == request.schema_hash_algorithm
        and (candidate.logical_data_hash == request.logical_data_hash)
        and candidate.schema_hash == request.schema_hash
    )


class PostgresDatasetPublicationCandidateStore:
    """Persist proposed public dataset revisions."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def register(self, request: DatasetPublicationCandidateRequest) -> DatasetPublicationCandidate:
        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO
                    catalog.dataset_publication_candidates (
                        candidate_id,
                        dataset_id,
                        version_period,
                        partition_key,
                        partition_value,
                        silver_build_id,
                        baseline_publication_id,
                        change_kind,
                        status,
                        fingerprint_version,
                        logical_hash_algorithm,
                        schema_hash_algorithm,
                        logical_data_hash,
                        schema_hash,
                        requested_at
                    )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    'awaiting_approval',
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (silver_build_id) DO NOTHING
                """,
                (
                    request.candidate_id,
                    request.dataset_id,
                    request.version_period,
                    request.partition_key,
                    request.partition_value,
                    request.silver_build_id,
                    request.baseline_publication_id,
                    request.change_kind.value,
                    request.fingerprint_version,
                    request.logical_hash_algorithm,
                    request.schema_hash_algorithm,
                    request.logical_data_hash,
                    request.schema_hash,
                    request.requested_at,
                ),
            )

            cursor.execute(
                f"""
                SELECT
                    {CANDIDATE_COLUMNS}
                FROM catalog.dataset_publication_candidates
                WHERE silver_build_id = %s
                """,
                (request.silver_build_id,),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Publication candidate was not registered")

        candidate = _row_to_candidate(row)

        if not _matches_request(candidate=candidate, request=request):
            raise RuntimeError(
                "The Silver build is already associated with a different publication candidate"
            )

        return candidate

    def get_by_id(self, candidate_id: str) -> DatasetPublicationCandidate | None:
        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {CANDIDATE_COLUMNS}
                FROM catalog.dataset_publication_candidates
                WHERE candidate_id = %s
                """,
                (candidate_id,),
            )

            row = cursor.fetchone()

        return _row_to_candidate(row) if row is not None else None

    def get_by_id_for_update(self, candidate_id: str) -> DatasetPublicationCandidate | None:
        normalized_id = _require_non_empty("candidate_id", candidate_id)

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {CANDIDATE_COLUMNS}
                FROM catalog.dataset_publication_candidates
                WHERE candidate_id = %s
                FOR UPDATE
                """,
                (normalized_id,),
            )

            row = cursor.fetchone()

        return _row_to_candidate(row) if row is not None else None

    def list_awaiting_approval(
        self, *, dataset_id: str | None = None
    ) -> list[DatasetPublicationCandidate]:
        parameters: tuple[object, ...]

        if dataset_id is None:
            where_clause = "status = 'awaiting_approval'"
            parameters = ()
        else:
            where_clause = """
                status = 'awaiting_approval'
                AND dataset_id = %s
            """
            parameters = (dataset_id,)

        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {CANDIDATE_COLUMNS}
                FROM catalog.dataset_publication_candidates
                WHERE {where_clause}
                ORDER BY requested_at, candidate_id
                """,
                parameters,
            )

            rows = cursor.fetchall()

        return [_row_to_candidate(row) for row in rows]

    def approve(
        self, *, candidate_id: str, approved_by: str, approved_at: datetime
    ) -> DatasetPublicationCandidate:
        normalized_id = _require_non_empty("candidate_id", candidate_id)
        normalized_actor = _require_non_empty("approved_by", approved_by)
        _require_aware_datetime("approved_at", approved_at)

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE catalog.dataset_publication_candidates
                SET
                    status = 'approved',
                    approved_at = %s,
                    approved_by = %s
                WHERE candidate_id = %s
                  AND status = 'awaiting_approval'
                RETURNING
                    {CANDIDATE_COLUMNS}
                """,
                (approved_at, normalized_actor, normalized_id),
            )

            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f"""
                    SELECT
                        {CANDIDATE_COLUMNS}
                    FROM catalog.dataset_publication_candidates
                    WHERE candidate_id = %s
                    """,
                    (normalized_id,),
                )

                row = cursor.fetchone()

        if row is None:
            raise KeyError(f"Unknown publication candidate: {normalized_id}")

        candidate = _row_to_candidate(row)

        if candidate.status is DatasetPublicationCandidateStatus.APPROVED:
            return candidate

        raise RuntimeError(
            "Only an awaiting_approval candidate can "
            "be approved. Candidate "
            f"{normalized_id} has status "
            f"{candidate.status.value}."
        )

    def reject(
        self, *, candidate_id: str, rejected_by: str, rejection_reason: str, rejected_at: datetime
    ) -> DatasetPublicationCandidate:
        normalized_id = _require_non_empty("candidate_id", candidate_id)
        normalized_actor = _require_non_empty("rejected_by", rejected_by)
        normalized_reason = _require_non_empty("rejection_reason", rejection_reason)
        _require_aware_datetime("rejected_at", rejected_at)

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE catalog.dataset_publication_candidates
                SET
                    status = 'rejected',
                    rejected_at = %s,
                    rejected_by = %s,
                    rejection_reason = %s
                WHERE candidate_id = %s
                  AND status = 'awaiting_approval'
                RETURNING
                    {CANDIDATE_COLUMNS}
                """,
                (rejected_at, normalized_actor, normalized_reason, normalized_id),
            )

            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f"""
                    SELECT
                        {CANDIDATE_COLUMNS}
                    FROM catalog.dataset_publication_candidates
                    WHERE candidate_id = %s
                    """,
                    (normalized_id,),
                )

                row = cursor.fetchone()

        if row is None:
            raise KeyError(f"Unknown publication candidate: {normalized_id}")

        candidate = _row_to_candidate(row)

        if candidate.status is DatasetPublicationCandidateStatus.REJECTED:
            return candidate

        raise RuntimeError(
            "Only an awaiting_approval candidate can "
            "be rejected. Candidate "
            f"{normalized_id} has status "
            f"{candidate.status.value}."
        )

    def mark_published(
        self, *, candidate_id: str, publication_id: str
    ) -> DatasetPublicationCandidate:
        normalized_candidate_id = _require_non_empty("candidate_id", candidate_id)
        normalized_publication_id = _require_non_empty("publication_id", publication_id)

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE catalog.dataset_publication_candidates
                SET
                    status = 'published',
                    publication_id = %s
                WHERE candidate_id = %s
                  AND status = 'approved'
                RETURNING
                    {CANDIDATE_COLUMNS}
                """,
                (normalized_publication_id, normalized_candidate_id),
            )

            row = cursor.fetchone()

            if row is None:
                cursor.execute(
                    f"""
                    SELECT
                        {CANDIDATE_COLUMNS}
                    FROM catalog.dataset_publication_candidates
                    WHERE candidate_id = %s
                    """,
                    (normalized_candidate_id,),
                )

                row = cursor.fetchone()

        if row is None:
            raise KeyError(f"Unknown publication candidate: {normalized_candidate_id}")

        candidate = _row_to_candidate(row)

        if (
            candidate.status is DatasetPublicationCandidateStatus.PUBLISHED
            and candidate.publication_id == normalized_publication_id
        ):
            return candidate

        raise RuntimeError(
            "Only an approved candidate can be published. "
            f"Candidate {normalized_candidate_id} has status "
            f"{candidate.status.value}."
        )
