from __future__ import annotations

import pytest

from metrka_core.metadata.migrations.config import resolve_migration_owner_role


def test_migration_owner_role_defaults_to_metrka_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("METRKA_MIGRATION_OWNER_ROLE", raising=False)

    assert resolve_migration_owner_role() == "metrka_owner"


def test_migration_owner_role_uses_configured_identifier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("METRKA_MIGRATION_OWNER_ROLE", "custom_owner")

    assert resolve_migration_owner_role() == "custom_owner"


@pytest.mark.parametrize("role", ["", "owner-role", "owner role", 'owner"role'])
def test_migration_owner_role_rejects_invalid_identifiers(role: str) -> None:
    with pytest.raises(
        ValueError, match="METRKA_MIGRATION_OWNER_ROLE must be a PostgreSQL identifier"
    ):
        resolve_migration_owner_role(role=role)
