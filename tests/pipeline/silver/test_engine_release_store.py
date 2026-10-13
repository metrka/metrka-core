from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from metrka_core.pipeline.silver.engine_store import (
    DEFAULT_ENGINE_RELEASE_LIST_LIMIT,
    MAX_ENGINE_RELEASE_LIST_LIMIT,
    require_engine_release_list_limit,
)
from metrka_core.pipeline.silver.postgres_engine_store import PostgresSilverEngineReleaseStore


@pytest.mark.parametrize("limit", [True, 1.5, "50", None])
def test_engine_release_limit_rejects_non_integer(limit: object) -> None:
    with pytest.raises(TypeError, match="limit must be an integer"):
        require_engine_release_list_limit(limit)  # type: ignore[arg-type]


@pytest.mark.parametrize("limit", [0, -1, MAX_ENGINE_RELEASE_LIST_LIMIT + 1])
def test_engine_release_limit_rejects_out_of_range_value(limit: int) -> None:
    with pytest.raises(ValueError, match="limit must be between"):
        require_engine_release_list_limit(limit)


def test_engine_release_limit_accepts_supported_boundaries() -> None:
    assert require_engine_release_list_limit(1) == 1
    assert (
        require_engine_release_list_limit(MAX_ENGINE_RELEASE_LIST_LIMIT)
        == MAX_ENGINE_RELEASE_LIST_LIMIT
    )


def test_postgres_engine_release_list_is_bounded_and_stably_ordered() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = cursor
    cursor_context.__exit__.return_value = False
    session = MagicMock()
    session.cursor.return_value = cursor_context

    result = PostgresSilverEngineReleaseStore(session).list_releases(limit=125)

    assert result == []
    query, parameters = cursor.execute.call_args.args
    assert "ORDER BY detected_at DESC, engine_release_id DESC" in query
    assert "LIMIT %s" in query
    assert parameters == (125,)


def test_default_engine_release_limit_is_operator_sized() -> None:
    assert DEFAULT_ENGINE_RELEASE_LIST_LIMIT == 50
