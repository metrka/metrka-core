"""Validation helpers for logical storage path segments."""

from __future__ import annotations


def require_path_segment(value: str, name: str) -> str:
    """Validate and return one logical path segment."""

    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")

    normalized = value.strip()

    if not normalized:
        raise ValueError(f"{name} must not be empty")

    if normalized in {".", ".."} or "/" in normalized or "\\" in normalized:
        raise ValueError(f"{name} must be a single path segment")

    return normalized
