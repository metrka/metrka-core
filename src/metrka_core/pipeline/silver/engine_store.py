"""Persistence contracts for Silver engine releases."""

from __future__ import annotations

from typing import Final

DEFAULT_ENGINE_RELEASE_LIST_LIMIT: Final = 50
MAX_ENGINE_RELEASE_LIST_LIMIT: Final = 1000


def require_engine_release_list_limit(limit: int) -> int:
    """Return one valid bounded administrative list size."""

    if isinstance(limit, bool) or not isinstance(limit, int):
        raise TypeError("limit must be an integer")

    if not 1 <= limit <= MAX_ENGINE_RELEASE_LIST_LIMIT:
        raise ValueError(
            f"limit must be between 1 and {MAX_ENGINE_RELEASE_LIST_LIMIT}, got {limit}"
        )

    return limit
