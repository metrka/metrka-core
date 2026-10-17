"""Tests for immutable PostgreSQL contract snapshot registration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import MagicMock

import pytest

from metrka_core.metadata.postgres_contract_metadata import PostgresContractMetadataStore

CONTRACT_HASH = "a" * 64


def test_identical_contract_snapshot_registration_is_idempotent() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    record = _record()
    cursor.fetchone.return_value = _stored_row(record)

    PostgresContractMetadataStore(session).register_contract_snapshot(record)

    assert cursor.execute.call_count == 2
    session.transaction.assert_called_once_with()

    insert_sql, insert_parameters = cursor.execute.call_args_list[0].args
    normalized_insert = " ".join(insert_sql.split())

    assert "ON CONFLICT (dataset_id, contract_hash) DO NOTHING" in normalized_insert
    assert "DO UPDATE" not in normalized_insert
    assert insert_parameters == _insert_parameters(record)

    select_sql, select_parameters = cursor.execute.call_args_list[1].args
    normalized_select = " ".join(select_sql.split())

    assert "FROM meta.contract_snapshots" in normalized_select
    assert "WHERE dataset_id = %s AND contract_hash = %s" in normalized_select
    assert select_parameters == ("demo.records", CONTRACT_HASH)


def test_same_contract_hash_can_be_registered_for_another_dataset() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    record = _record(dataset="another", dataset_id="another.records")
    cursor.fetchone.return_value = _stored_row(record)

    PostgresContractMetadataStore(session).register_contract_snapshot(record)

    _, select_parameters = cursor.execute.call_args_list[1].args

    assert select_parameters == ("another.records", CONTRACT_HASH)


def test_conflicting_metadata_for_same_dataset_and_hash_is_rejected() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value

    incoming = _record()
    existing = {**incoming, "contract_version": "2.0.0"}
    cursor.fetchone.return_value = _stored_row(existing)

    with pytest.raises(RuntimeError, match="different immutable metadata"):
        PostgresContractMetadataStore(session).register_contract_snapshot(incoming)


def test_missing_persisted_contract_snapshot_is_rejected() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None

    with pytest.raises(RuntimeError, match="registration was not persisted"):
        PostgresContractMetadataStore(session).register_contract_snapshot(_record())


def test_dataset_id_is_required() -> None:
    session = MagicMock()
    record = {**_record(), "dataset_id": None}

    with pytest.raises(ValueError, match="dataset_id must be a non-empty string"):
        PostgresContractMetadataStore(session).register_contract_snapshot(record)

    session.transaction.assert_not_called()


def _record(*, dataset: str = "demo", dataset_id: str = "demo.records") -> dict[str, Any]:
    snapshot_root = f"contracts/{dataset_id}/sha256={CONTRACT_HASH}"

    return {
        "contract_hash": CONTRACT_HASH,
        "dataset": dataset,
        "dataset_id": dataset_id,
        "contract_name": "records.yaml",
        "contract_stem": "records",
        "contract_path": "conf/records.yaml",
        "contract_version": "1.0.0",
        "contract_snapshot_yaml_path": f"{snapshot_root}/contract.yaml",
        "contract_snapshot_json_path": f"{snapshot_root}/contract.json",
    }


def _stored_row(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract_hash": record["contract_hash"],
        "dataset": record["dataset"],
        "dataset_id": record["dataset_id"],
        "contract_name": record["contract_name"],
        "contract_stem": record["contract_stem"],
        "contract_path": record["contract_path"],
        "contract_version": record["contract_version"],
        "snapshot_yaml_path": record["contract_snapshot_yaml_path"],
        "snapshot_json_path": record["contract_snapshot_json_path"],
    }


def _insert_parameters(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record["contract_hash"],
        record["dataset"],
        record["dataset_id"],
        record["contract_name"],
        record["contract_stem"],
        record["contract_path"],
        record["contract_version"],
        record["contract_snapshot_yaml_path"],
        record["contract_snapshot_json_path"],
    )
