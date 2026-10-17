"""PostgreSQL implementation of contract metadata storage."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from metrka_core.metadata.postgres import PostgresSession


class PostgresContractMetadataStore:
    """Persist immutable dataset-to-contract snapshot registrations."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def register_contract_snapshot(self, record: dict[str, Any]) -> None:
        """Insert or verify one immutable dataset-to-contract registration.

        Repeating the same registration is idempotent. The same contract hash
        may be registered for different datasets. Conflicting metadata for the
        same dataset and contract hash is rejected.
        """

        dataset_id = record.get("dataset_id")
        if not isinstance(dataset_id, str) or not dataset_id.strip():
            raise ValueError("dataset_id must be a non-empty string")

        contract_hash = record["contract_hash"]
        expected_identity = _record_identity(record)

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO meta.contract_snapshots (
                    contract_hash,
                    dataset,
                    dataset_id,
                    contract_name,
                    contract_stem,
                    contract_path,
                    contract_version,
                    snapshot_yaml_path,
                    snapshot_json_path
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (dataset_id, contract_hash)
                DO NOTHING
                """,
                expected_identity,
            )

            cursor.execute(
                """
                SELECT
                    contract_hash,
                    dataset,
                    dataset_id,
                    contract_name,
                    contract_stem,
                    contract_path,
                    contract_version,
                    snapshot_yaml_path,
                    snapshot_json_path
                FROM meta.contract_snapshots
                WHERE dataset_id = %s
                  AND contract_hash = %s
                """,
                (dataset_id, contract_hash),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "Contract snapshot registration was not persisted: "
                    f"dataset_id={dataset_id}, contract_hash={contract_hash}"
                )

            if not isinstance(row, Mapping):
                raise RuntimeError("Contract snapshot lookup returned an invalid database row")

            if _row_identity(row) != expected_identity:
                raise RuntimeError(
                    "Contract snapshot registration already exists with "
                    "different immutable metadata: "
                    f"dataset_id={dataset_id}, contract_hash={contract_hash}"
                )


def _row_identity(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        row["contract_hash"],
        row["dataset"],
        row["dataset_id"],
        row["contract_name"],
        row["contract_stem"],
        row["contract_path"],
        row["contract_version"],
        row["snapshot_yaml_path"],
        row["snapshot_json_path"],
    )


def _record_identity(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record["contract_hash"],
        record["dataset"],
        record["dataset_id"],
        record["contract_name"],
        record["contract_stem"],
        record["contract_path"],
        record.get("contract_version"),
        record["contract_snapshot_yaml_path"],
        record.get("contract_snapshot_json_path"),
    )
