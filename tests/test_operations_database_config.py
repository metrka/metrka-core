from __future__ import annotations

import pytest

from metrka_core.operations.database_config import (
    OPERATIONS_DSN_ENVIRONMENT_VARIABLE,
    resolve_operations_conninfo,
)


def test_explicit_operations_conninfo_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPERATIONS_DSN_ENVIRONMENT_VARIABLE, "postgresql://environment-operator")

    assert (
        resolve_operations_conninfo(conninfo="  postgresql://explicit-operator  ")
        == "postgresql://explicit-operator"
    )


def test_operations_conninfo_uses_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(OPERATIONS_DSN_ENVIRONMENT_VARIABLE, "  postgresql://environment-operator  ")

    assert resolve_operations_conninfo() == "postgresql://environment-operator"


def test_empty_explicit_operations_conninfo_is_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        resolve_operations_conninfo(conninfo="   ")


@pytest.mark.parametrize("environment_value", [None, "", "   "])
def test_missing_operations_conninfo_has_actionable_error(
    monkeypatch: pytest.MonkeyPatch, environment_value: str | None
) -> None:
    if environment_value is None:
        monkeypatch.delenv(OPERATIONS_DSN_ENVIRONMENT_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(OPERATIONS_DSN_ENVIRONMENT_VARIABLE, environment_value)

    with pytest.raises(RuntimeError, match=OPERATIONS_DSN_ENVIRONMENT_VARIABLE):
        resolve_operations_conninfo()
