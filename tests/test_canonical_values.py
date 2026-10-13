from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import numpy as np
import pandas as pd
import pytest

from metrka_core.values.canonical import canonical_fingerprint_scalar, json_scalar


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (pd.NA, None),
        (pd.NaT, None),
        (np.int64(7), 7),
        (np.float64(1.5), 1.5),
        (Decimal("1.50"), "1.50"),
        (date(2026, 8, 16), "2026-08-16"),
        (datetime(2026, 8, 16, 12, 30, tzinfo=UTC), "2026-08-16T12:30:00+00:00"),
        (pd.Timestamp("2026-08-16T12:30:00Z"), "2026-08-16T12:30:00+00:00"),
        (timedelta(days=1, seconds=2), "P1DT0H0M2S"),
        (pd.Period("2026-08", freq="M"), "2026-08"),
        (UUID("11111111-1111-4111-8111-111111111111"), "11111111-1111-4111-8111-111111111111"),
        (b"\x01\xff", "01ff"),
    ],
)
def test_json_scalar_returns_json_safe_value(value: object, expected: object) -> None:
    normalized = json_scalar(value)

    assert normalized == expected
    json.dumps(normalized, allow_nan=False)


def test_json_scalar_normalizes_unicode() -> None:
    assert json_scalar("Cafe\u0301") == "Café"


def test_json_scalar_rejects_unknown_item_protocol() -> None:
    class ItemImpostor:
        def item(self) -> str:
            return "not a numpy scalar"

    with pytest.raises(TypeError, match="Unsupported JSON scalar value type: ItemImpostor"):
        json_scalar(ItemImpostor())


def test_fingerprint_scalar_contract_is_available_from_shared_module() -> None:
    assert canonical_fingerprint_scalar(np.int64(7)) == ["integer", "7"]
