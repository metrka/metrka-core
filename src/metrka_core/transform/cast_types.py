"""Canonical vocabulary for Silver contract cast types."""

from __future__ import annotations

from enum import StrEnum


class SilverCastType(StrEnum):
    """Cast type names accepted in a Silver data contract."""

    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"


SILVER_CAST_TYPE_NAMES = frozenset(cast_type.value for cast_type in SilverCastType)

DATE_LIKE_INPUT_TYPES = frozenset(
    {SilverCastType.DATE, SilverCastType.DATETIME, SilverCastType.TIMESTAMP}
)

CANONICAL_DATE_CAST_TYPES = frozenset({SilverCastType.DATE, SilverCastType.DATETIME})


def parse_silver_cast_type(value: object) -> SilverCastType | None:
    """Return a declared Silver cast type, or None for an unknown value."""

    if not isinstance(value, str):
        return None

    try:
        return SilverCastType(value)
    except ValueError:
        return None


def canonicalize_silver_cast_type(cast_type: SilverCastType) -> SilverCastType:
    """Normalize public aliases to the internal canonical vocabulary."""

    if cast_type is SilverCastType.TIMESTAMP:
        return SilverCastType.DATETIME

    return cast_type


def resolve_silver_cast_type(value: object) -> SilverCastType:
    """Parse and canonicalize a cast type, rejecting unknown values."""

    cast_type = parse_silver_cast_type(value)

    if cast_type is None:
        raise ValueError(f"Unsupported Silver cast type: {value!r}")

    return canonicalize_silver_cast_type(cast_type)


def is_date_like_cast_type(value: object) -> bool:
    """Return whether a contract value belongs to the date-processing route."""

    cast_type = parse_silver_cast_type(value)
    return cast_type in DATE_LIKE_INPUT_TYPES
