"""Canonical human-readable names for filesystem projections."""

from __future__ import annotations

import re

_DATASET_ID_COMPONENT = r"[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?"
_DATASET_ID_PATTERN = re.compile(rf"^{_DATASET_ID_COMPONENT}(?:\.{_DATASET_ID_COMPONENT})*$")
_MAX_DATASET_ID_LENGTH = 200


def pointer_file_name(dataset_id: str) -> str:
    """Return the canonical readable filename for one dataset pointer."""

    if not isinstance(dataset_id, str):
        raise TypeError("dataset_id must be a string")

    if not dataset_id:
        raise ValueError("dataset_id is required")

    if dataset_id != dataset_id.strip():
        raise ValueError("dataset_id must not contain surrounding whitespace")

    if len(dataset_id) > _MAX_DATASET_ID_LENGTH:
        raise ValueError(f"dataset_id must not exceed {_MAX_DATASET_ID_LENGTH} characters")

    if _DATASET_ID_PATTERN.fullmatch(dataset_id) is None:
        raise ValueError(
            "dataset_id must contain lowercase hierarchy components separated by dots; "
            "components may contain lowercase letters, digits, underscores and hyphens"
        )

    return f"dataset--{dataset_id}.json"
