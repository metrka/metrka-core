"""Strict canonical representations for scalar values."""

from __future__ import annotations

import math
import unicodedata
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import numpy as np
import pandas as pd

type JsonScalar = str | int | float | bool | None


def canonical_tagged_scalar(value: object) -> object:
    """Return the shared tagged V1 representation for a supported scalar value."""

    if value is None or value is pd.NA or value is pd.NaT:
        return ["null", None]

    if isinstance(value, np.generic):
        value = value.item()

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return ["null", None]

        if value.tzinfo is not None:
            value = value.tz_convert("UTC")

        return ["datetime", value.isoformat()]

    if isinstance(value, datetime):
        return ["datetime", value.isoformat()]

    if isinstance(value, date):
        return ["date", value.isoformat()]

    if isinstance(value, Decimal):
        if value.is_nan():
            return ["null", None]

        return ["decimal", format(value, "f")]

    if isinstance(value, bool):
        return ["boolean", value]

    if isinstance(value, int):
        return ["integer", str(value)]

    if isinstance(value, float):
        if math.isnan(value):
            return ["null", None]

        if math.isinf(value):
            return ["float", "positive_infinity" if value > 0 else "negative_infinity"]

        return ["float", value.hex()]

    if isinstance(value, str):
        return ["string", unicodedata.normalize("NFC", value)]

    if isinstance(value, bytes):
        return ["bytes", value.hex()]

    if isinstance(value, UUID):
        return ["uuid", str(value)]

    if isinstance(value, Mapping):
        return [
            "mapping",
            {
                str(key): canonical_tagged_scalar(item)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            },
        ]

    if isinstance(value, (list, tuple)):
        return ["sequence", [canonical_tagged_scalar(item) for item in value]]

    raise TypeError(f"Unsupported canonical scalar value type: {type(value).__name__}")


def canonical_fingerprint_scalar(value: object) -> object:
    """Return the tagged V1 scalar representation used by Silver fingerprints."""

    return canonical_tagged_scalar(value)


def json_scalar(value: object) -> JsonScalar:
    """Return a strict JSON-compatible scalar for public metadata."""

    if value is None or value is pd.NA or value is pd.NaT:
        return None

    if isinstance(value, np.generic):
        return json_scalar(value.item())

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None

        return value.isoformat()

    if isinstance(value, pd.Timedelta):
        if pd.isna(value):
            return None

        return value.isoformat()

    if isinstance(value, pd.Period):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, timedelta):
        return pd.Timedelta(value).isoformat()

    if isinstance(value, Decimal):
        if value.is_nan():
            return None

        if value.is_infinite():
            return "Infinity" if value > 0 else "-Infinity"

        return format(value, "f")

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if math.isnan(value):
            return None

        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"

        return value

    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)

    if isinstance(value, bytes):
        return value.hex()

    if isinstance(value, UUID):
        return str(value)

    raise TypeError(f"Unsupported JSON scalar value type: {type(value).__name__}")
