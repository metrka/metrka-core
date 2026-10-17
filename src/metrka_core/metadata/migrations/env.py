"""Alembic runtime environment for the Metrka metadata database."""

from __future__ import annotations

from typing import Any

import psycopg
from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import Connection, Engine

from metrka_core.metadata.migrations.config import (
    resolve_migration_conninfo,
    resolve_migration_owner_role,
)

config = context.config
target_metadata = None


def _configured_conninfo() -> str:
    configured = config.attributes.get("metadata_conninfo")

    if configured is None:
        return resolve_migration_conninfo()

    if not isinstance(configured, str):
        raise TypeError("Alembic metadata_conninfo must be a string")

    return resolve_migration_conninfo(conninfo=configured)


def _create_engine() -> Engine:
    conninfo = _configured_conninfo()

    def connect() -> Any:
        return psycopg.connect(conninfo)

    return create_engine("postgresql+psycopg://", creator=connect, poolclass=pool.NullPool)


def _assume_owner_role(connection: Connection) -> None:
    role = resolve_migration_owner_role()
    connection.exec_driver_sql(f'SET ROLE "{role}"')
    connection.commit()


def run_migrations_offline() -> None:
    """Render PostgreSQL SQL without opening a database connection."""

    context.configure(
        url="postgresql+psycopg://",
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        version_table="alembic_version",
        version_table_schema="public",
        transactional_ddl=True,
    )

    with context.begin_transaction():
        context.execute(f'SET ROLE "{resolve_migration_owner_role()}"')
        context.run_migrations()


def run_migrations_online() -> None:
    """Apply migrations as the non-login metadata owner role."""

    engine = _create_engine()

    try:
        with engine.connect() as connection:
            _assume_owner_role(connection)

            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                include_schemas=True,
                compare_type=True,
                version_table="alembic_version",
                version_table_schema="public",
                transactional_ddl=True,
                transaction_per_migration=True,
            )

            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
