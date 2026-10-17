"""Create the complete public Metrka 1.0 metadata schema."""

from __future__ import annotations

from collections.abc import Sequence
from importlib.resources import files

import sqlalchemy as sa
import sqlparse
from alembic import op

revision: str = "0001_initial"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _execute_resource(resource_name: str) -> None:
    sql = (
        files("metrka_core.metadata.migrations.sql")
        .joinpath(resource_name)
        .read_text(encoding="utf-8")
    )

    for statement in sqlparse.split(sql):
        normalized = statement.strip()
        if normalized:
            op.execute(sa.text(normalized))


def upgrade() -> None:
    _execute_resource("0001_initial.sql")
    _execute_resource("0001_reference_data.sql")
    _execute_resource("0001_permissions.sql")


def downgrade() -> None:
    raise RuntimeError(
        "The initial metadata schema cannot be downgraded automatically. "
        "Restore a backup or recreate the database."
    )
