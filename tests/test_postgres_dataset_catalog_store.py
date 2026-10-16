"""Tests for catalog categories and tags registered from Silver contracts."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from metrka_core.catalog.postgres_dataset_catalog import PostgresDatasetCatalogStore

CONTRACT_HASH = "c" * 64
DATASET_ID = "fl_healthcharts.drug_confirmed_motor_vehicle_traffic_crash_injuries"


def test_tags_are_stored_in_contract_order_with_their_contract_text() -> None:
    session, cursor = _session()

    _register(session, tags=["injuries", "drugs", " traffic crashes ", "florida", "county"])

    assert _membership_rows(cursor) == [
        (DATASET_ID, "injuries", 1, "injuries", CONTRACT_HASH),
        (DATASET_ID, "drugs", 2, "drugs", CONTRACT_HASH),
        (DATASET_ID, "traffic-crashes", 3, "traffic crashes", CONTRACT_HASH),
        (DATASET_ID, "florida", 4, "florida", CONTRACT_HASH),
        (DATASET_ID, "county", 5, "county", CONTRACT_HASH),
    ]
    assert _registered_tag_slugs(cursor) == [
        "injuries",
        "drugs",
        "traffic-crashes",
        "florida",
        "county",
    ]


def test_tags_with_one_slug_keep_the_first_place_and_text() -> None:
    session, cursor = _session()

    _register(session, tags=["traffic crashes", "florida", "traffic-crashes", "florida"])

    assert _membership_rows(cursor) == [
        (DATASET_ID, "traffic-crashes", 1, "traffic crashes", CONTRACT_HASH),
        (DATASET_ID, "florida", 2, "florida", CONTRACT_HASH),
    ]


def test_a_changed_contract_rewrites_the_place_and_text_of_its_tags() -> None:
    session, cursor = _session()

    _register(session, tags=["county"])

    statements = [" ".join(call.args[0].split()) for call in cursor.execute.call_args_list]
    delete_index = _statement_index(statements, "DELETE FROM catalog.dataset_tag_memberships")
    upsert_index = _statement_index(statements, "INSERT INTO catalog.dataset_tag_memberships")
    upsert = statements[upsert_index]

    assert delete_index < upsert_index
    assert "ON CONFLICT ( dataset_id, tag_slug ) DO UPDATE SET" in upsert
    assert "tag_position = EXCLUDED.tag_position" in upsert
    assert "tag_label = EXCLUDED.tag_label" in upsert


def test_a_contract_without_tags_removes_its_tags() -> None:
    session, cursor = _session()

    _register(session, tags=[])

    assert any(
        "DELETE FROM catalog.dataset_tag_memberships" in call.args[0]
        for call in cursor.execute.call_args_list
    )
    assert _membership_rows(cursor) == []


@pytest.mark.parametrize(
    ("tags", "message"),
    [
        (["florida", 7], "non-empty strings"),
        (["florida", "  "], "non-empty strings"),
        (["florida", "---"], "valid tag slugs"),
    ],
)
def test_invalid_tags_are_rejected_before_the_catalog_changes(
    tags: list[object], message: str
) -> None:
    session, _ = _session()

    with pytest.raises(ValueError, match=message):
        _register(session, tags=tags)

    session.transaction.assert_not_called()


def _session() -> tuple[MagicMock, MagicMock]:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"category_slug": "health-medicine"}
    return session, cursor


def _register(session: MagicMock, *, tags: list[Any]) -> None:
    PostgresDatasetCatalogStore(session).register_dataset_catalog_from_contract(
        dataset_id=DATASET_ID,
        contract_hash=CONTRACT_HASH,
        contract={"meta": {"category": "health-medicine", "tags": tags}},
    )


def _statement_index(statements: list[str], prefix: str) -> int:
    return next(index for index, sql in enumerate(statements) if sql.startswith(prefix))


def _membership_calls(cursor: MagicMock) -> list[Any]:
    return [
        call
        for call in cursor.execute.call_args_list
        if "INSERT INTO catalog.dataset_tag_memberships" in call.args[0]
    ]


def _membership_rows(cursor: MagicMock) -> list[tuple[object, ...]]:
    return [call.args[1] for call in _membership_calls(cursor)]


def _registered_tag_slugs(cursor: MagicMock) -> list[object]:
    return [
        call.args[1][0]
        for call in cursor.execute.call_args_list
        if "INSERT INTO catalog.dataset_tags" in call.args[0]
    ]
