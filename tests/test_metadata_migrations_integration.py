from __future__ import annotations

import os
from uuid import uuid4

import psycopg
import pytest

from metrka_core.metadata.postgres import PostgresSession
from metrka_core.metadata.schema_compatibility import inspect_metadata_schema
from metrka_core.pipeline.acquisition.models import SourceCaptureAssetBinding
from metrka_core.pipeline.acquisition.postgres_source_capture_store import (
    PostgresSourceCaptureStore,
)

TEST_DSN = os.environ.get("METRKA_MIGRATION_TEST_DSN")
OPERATIONS_DSN = os.environ.get("METRKA_OPERATIONS_DSN")
EXPECTED_OWNER = os.environ.get("METRKA_MIGRATION_OWNER_ROLE", "metrka_owner")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_DSN, reason="METRKA_MIGRATION_TEST_DSN is not configured"),
]

EXPECTED_TABLES = {
    ("catalog", "dataset_publication_assets"),
    ("catalog", "dataset_publication_candidates"),
    ("catalog", "dataset_publication_projection_states"),
    ("catalog", "dataset_publications"),
    ("logs", "execution_logs"),
    ("logs", "marshal_events"),
    ("logs", "pipeline_runs"),
    ("logs", "silver_build_attempts"),
    ("quality", "silver_publication_verifications"),
    ("meta", "source_capture_assets"),
    ("meta", "source_captures"),
    ("lineage", "transformation_impacts"),
    ("meta", "contract_snapshots"),
    ("catalog", "dataset_categories"),
    ("catalog", "dataset_category_memberships"),
    ("catalog", "dataset_tags"),
    ("catalog", "dataset_tag_memberships"),
    ("meta", "marshaled_files"),
    ("meta", "silver_materializations"),
    ("meta", "silver_engine_releases"),
    ("meta", "source_schema_bindings"),
    ("meta", "source_schema_fields"),
    ("meta", "source_schema_snapshots"),
    ("quality", "quality_check_definitions"),
    ("quality", "quality_check_runs"),
    ("quality", "asset_integrity_batches"),
    ("quality", "asset_integrity_results"),
    ("quality", "publication_gate_attempts"),
    ("quality", "publication_integrity_checks"),
}


def _test_dsn() -> str:
    if TEST_DSN is None:
        raise RuntimeError("Migration test DSN is missing")
    return TEST_DSN


def test_migrations_create_every_required_table() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
           WHERE table_schema IN (
            'catalog',
            'lineage',
            'logs',
            'meta',
            'quality'
        )
            """
        )
        actual = {
            (str(schema_name), str(table_name)) for schema_name, table_name in cursor.fetchall()
        }

    assert actual == EXPECTED_TABLES


def test_all_application_tables_have_primary_keys() -> None:
    """Keep row identity explicit for every mutable Metrka table."""

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                namespace.nspname,
                relation.relname
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname IN (
                    'catalog',
                    'lineage',
                    'logs',
                    'meta',
                    'quality'
                  )
              AND relation.relkind IN ('r', 'p')
              AND NOT EXISTS (
                    SELECT 1
                    FROM pg_index AS table_index
                    WHERE table_index.indrelid = relation.oid
                      AND table_index.indisprimary
                  )
            ORDER BY namespace.nspname, relation.relname
            """
        )
        tables_without_primary_keys = [
            (str(schema_name), str(table_name)) for schema_name, table_name in cursor.fetchall()
        ]

    assert tables_without_primary_keys == []


def test_all_metadata_objects_have_one_owner() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                namespace.nspname,
                relation.relname,
                pg_get_userbyid(relation.relowner)
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE (
                    namespace.nspname IN ('catalog', 'lineage', 'logs', 'meta', 'quality')
                    OR (
                        namespace.nspname = 'public'
                        AND relation.relname = 'alembic_version'
                    )
                  )
              AND relation.relkind IN ('r', 'p', 'S', 'v', 'm')
            ORDER BY namespace.nspname, relation.relname
            """
        )
        objects = cursor.fetchall()

    wrong_owners = [
        (str(schema), str(name), str(owner))
        for schema, name, owner in objects
        if str(owner) != EXPECTED_OWNER
    ]

    assert wrong_owners == []


def test_database_and_application_schemas_have_one_owner() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT current_database(),
                   pg_get_userbyid(database.datdba)
            FROM pg_database AS database
            WHERE database.datname = current_database()
            """
        )
        database_name, database_owner = cursor.fetchone()

        cursor.execute(
            """
            SELECT namespace.nspname,
                   pg_get_userbyid(namespace.nspowner)
            FROM pg_namespace AS namespace
            WHERE namespace.nspname IN ('catalog', 'lineage', 'logs', 'meta', 'quality')
            ORDER BY namespace.nspname
            """
        )
        schemas = cursor.fetchall()

    assert str(database_owner) == EXPECTED_OWNER, str(database_name)
    assert {(str(schema_name), str(owner)) for schema_name, owner in schemas} == {
        ("catalog", EXPECTED_OWNER),
        ("lineage", EXPECTED_OWNER),
        ("logs", EXPECTED_OWNER),
        ("meta", EXPECTED_OWNER),
        ("quality", EXPECTED_OWNER),
    }


def test_operator_role_is_login_without_owner_membership() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                rolcanlogin,
                rolsuper,
                rolcreatedb,
                rolcreaterole,
                rolinherit
            FROM pg_roles
            WHERE rolname = 'metrka_operator'
            """
        )
        attributes = cursor.fetchone()

        cursor.execute(
            """
            SELECT pg_has_role(
                'metrka_operator',
                'metrka_owner',
                'MEMBER'
            )
            """
        )
        owner_membership = cursor.fetchone()

    assert attributes == (True, False, False, False, False)
    assert owner_membership == (False,)


@pytest.mark.skipif(not OPERATIONS_DSN, reason="METRKA_OPERATIONS_DSN is not configured")
def test_operator_connection_can_read_but_cannot_assume_owner() -> None:
    if OPERATIONS_DSN is None:
        raise RuntimeError("Operator test DSN is missing")

    with (
        psycopg.connect(OPERATIONS_DSN, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT current_user")
        assert cursor.fetchone() == ("metrka_operator",)

        cursor.execute("SELECT count(*) FROM catalog.dataset_publications")
        publication_count = cursor.fetchone()
        assert publication_count is not None
        assert isinstance(publication_count[0], int)
        assert publication_count[0] >= 0

        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cursor.execute("SET ROLE metrka_owner")


def test_operator_role_has_read_access_and_only_governance_writes() -> None:
    insert_tables = {
        ("catalog", "dataset_publication_assets"),
        ("catalog", "dataset_publication_projection_states"),
        ("catalog", "dataset_publications"),
        ("quality", "asset_integrity_batches"),
        ("quality", "asset_integrity_results"),
        ("quality", "publication_gate_attempts"),
        ("quality", "publication_integrity_checks"),
    }
    update_tables = {
        ("catalog", "dataset_publication_candidates"),
        ("catalog", "dataset_publication_projection_states"),
        ("catalog", "dataset_publications"),
        ("meta", "silver_engine_releases"),
    }

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                table_schema,
                table_name,
                has_table_privilege(
                    'metrka_operator',
                    format('%I.%I', table_schema, table_name),
                    'SELECT'
                ),
                has_table_privilege(
                    'metrka_operator',
                    format('%I.%I', table_schema, table_name),
                    'INSERT'
                ),
                has_table_privilege(
                    'metrka_operator',
                    format('%I.%I', table_schema, table_name),
                    'UPDATE'
                ),
                has_table_privilege(
                    'metrka_operator',
                    format('%I.%I', table_schema, table_name),
                    'DELETE'
                )
            FROM information_schema.tables
            WHERE table_schema IN ('catalog', 'lineage', 'logs', 'meta', 'quality')
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
            """
        )
        privileges = cursor.fetchall()

        cursor.execute(
            """
            SELECT
                schema_name,
                has_schema_privilege(
                    'metrka_operator',
                    schema_name,
                    'USAGE'
                ),
                has_schema_privilege(
                    'metrka_operator',
                    schema_name,
                    'CREATE'
                )
            FROM unnest(ARRAY['catalog', 'lineage', 'logs', 'meta', 'quality'])
                 AS application_schemas(schema_name)
            ORDER BY schema_name
            """
        )
        schema_privileges = cursor.fetchall()

    assert len(privileges) == len(EXPECTED_TABLES)

    for schema_name, table_name, can_select, can_insert, can_update, can_delete in privileges:
        table = (str(schema_name), str(table_name))
        assert can_select is True, table
        assert can_insert is (table in insert_tables), table
        assert can_update is (table in update_tables), table
        assert can_delete is False, table

    assert schema_privileges == [
        ("catalog", True, False),
        ("lineage", True, False),
        ("logs", True, False),
        ("meta", True, False),
        ("quality", True, False),
    ]


def test_operator_role_has_only_required_sequence_access() -> None:
    expected_sequences = {
        ("quality", "asset_integrity_batches_integrity_batch_id_seq"),
        ("quality", "publication_gate_attempts_gate_attempt_id_seq"),
    }

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                namespace.nspname,
                relation.relname,
                has_sequence_privilege(
                    'metrka_operator',
                    format('%I.%I', namespace.nspname, relation.relname),
                    'USAGE'
                )
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname IN ('catalog', 'lineage', 'logs', 'meta', 'quality')
              AND relation.relkind = 'S'
            ORDER BY namespace.nspname, relation.relname
            """
        )
        privileges = cursor.fetchall()

    assert {
        (str(schema_name), str(sequence_name))
        for schema_name, sequence_name, has_usage in privileges
        if has_usage
    } == expected_sequences


def test_migrated_database_is_current() -> None:
    with PostgresSession(conninfo=_test_dsn()) as session:
        status = inspect_metadata_schema(session)

    assert status.is_current


def test_placeholder_provenance_columns_are_not_in_v1_schema() -> None:
    removed_columns = {
        ("meta", "marshaled_files", "schema_signature_hash"),
        ("meta", "marshaled_files", "processing_config_version"),
        ("meta", "marshaled_files", "pipeline_version"),
        ("quality", "quality_check_runs", "code_version"),
        ("quality", "quality_check_runs", "params_hash"),
    }

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name, column_name
            FROM information_schema.columns
            WHERE (
                    table_schema = 'meta'
                    AND table_name = 'marshaled_files'
                  )
               OR (
                    table_schema = 'quality'
                    AND table_name = 'quality_check_runs'
                  )
            """
        )
        actual_columns = {
            (str(schema_name), str(table_name), str(column_name))
            for schema_name, table_name, column_name in cursor.fetchall()
        }

    assert actual_columns.isdisjoint(removed_columns)


def test_source_schema_snapshots_persist_hash_algorithm() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'meta'
              AND table_name = 'source_schema_snapshots'
              AND column_name = 'schema_hash_algorithm'
            """
        )
        column = cursor.fetchone()

    assert column == ("text", "NO")


def test_source_schema_snapshots_persist_field_binding() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'meta'
              AND table_name = 'source_schema_snapshots'
              AND column_name = 'field_binding'
            """
        )
        column = cursor.fetchone()

    assert column == ("text", "NO")


def test_execution_logs_have_stable_identity_and_required_event_fields() -> None:
    required_columns = {
        "ts",
        "schema_version",
        "dataset",
        "layer",
        "step",
        "run_id",
        "step_id",
        "event_type",
    }

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                column_name,
                is_nullable,
                is_identity,
                identity_generation
            FROM information_schema.columns
            WHERE table_schema = 'logs'
              AND table_name = 'execution_logs'
              AND (
                    column_name = 'execution_event_id'
                    OR column_name = ANY(%s)
                  )
            """,
            (sorted(required_columns),),
        )
        columns = {
            str(column_name): (
                str(is_nullable),
                str(is_identity),
                None if identity_generation is None else str(identity_generation),
            )
            for column_name, is_nullable, is_identity, identity_generation in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = 'logs'
              AND table_name = 'execution_logs'
              AND constraint_name IN (
                    'execution_logs_pkey',
                    'execution_logs_run_step_event_key'
                  )
            """
        )
        constraints = {
            str(constraint_name): str(constraint_type)
            for constraint_name, constraint_type in cursor.fetchall()
        }

    assert columns["execution_event_id"] == ("NO", "YES", "ALWAYS")
    assert all(columns[column_name][0] == "NO" for column_name in required_columns)
    assert constraints == {
        "execution_logs_pkey": "PRIMARY KEY",
        "execution_logs_run_step_event_key": "UNIQUE",
    }


def test_marshal_events_have_stable_identity_and_required_event_fields() -> None:
    required_columns = {"event_ts", "event_type", "file_id", "reason", "new_entry", "meta"}

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                column_name,
                is_nullable,
                is_identity,
                identity_generation
            FROM information_schema.columns
            WHERE table_schema = 'logs'
              AND table_name = 'marshal_events'
              AND (
                    column_name = 'marshal_event_id'
                    OR column_name = ANY(%s)
                  )
            """,
            (sorted(required_columns),),
        )
        columns = {
            str(column_name): (
                str(is_nullable),
                str(is_identity),
                None if identity_generation is None else str(identity_generation),
            )
            for column_name, is_nullable, is_identity, identity_generation in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = 'logs'
              AND table_name = 'marshal_events'
              AND constraint_name = 'marshal_events_pkey'
            """
        )
        constraints = {
            str(constraint_name): str(constraint_type)
            for constraint_name, constraint_type in cursor.fetchall()
        }

    assert columns["marshal_event_id"] == ("NO", "YES", "ALWAYS")
    assert all(columns[column_name][0] == "NO" for column_name in required_columns)
    assert constraints == {"marshal_events_pkey": "PRIMARY KEY"}


def test_asset_integrity_evidence_is_normalized_keyed_and_append_only() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_name, constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema = 'quality'
              AND table_name IN (
                    'asset_integrity_batches',
                    'asset_integrity_results',
                    'publication_gate_attempts',
                    'publication_integrity_checks'
                  )
              AND constraint_name IN (
                    'asset_integrity_batches_pkey',
                    'asset_integrity_results_pkey',
                    'asset_integrity_results_batch_fkey',
                    'publication_gate_attempts_pkey',
                    'publication_gate_attempts_candidate_fkey',
                    'publication_gate_attempts_pipeline_run_fkey',
                    'publication_gate_attempts_integrity_batch_fkey',
                    'publication_integrity_checks_pkey',
                    'publication_integrity_checks_publication_fkey',
                    'publication_integrity_checks_batch_fkey'
                  )
            """
        )
        constraints = {
            (str(table_name), str(constraint_name)): str(constraint_type)
            for table_name, constraint_name, constraint_type in cursor.fetchall()
        }

        cursor.execute(
            """
            SELECT
                table_name,
                has_table_privilege(
                    'metrka_etl',
                    format('quality.%I', table_name),
                    'SELECT'
                ),
                has_table_privilege(
                    'metrka_etl',
                    format('quality.%I', table_name),
                    'INSERT'
                ),
                has_table_privilege(
                    'metrka_etl',
                    format('quality.%I', table_name),
                    'UPDATE'
                ),
                has_table_privilege(
                    'metrka_etl',
                    format('quality.%I', table_name),
                    'DELETE'
                )
            FROM (
                VALUES
                    ('asset_integrity_batches'),
                    ('asset_integrity_results'),
                    ('publication_gate_attempts'),
                    ('publication_integrity_checks')
            ) AS evidence_tables(table_name)
            ORDER BY table_name
            """
        )
        privileges = {
            str(table_name): (can_select, can_insert, can_update, can_delete)
            for table_name, can_select, can_insert, can_update, can_delete in cursor.fetchall()
        }

    assert constraints == {
        ("asset_integrity_batches", "asset_integrity_batches_pkey"): "PRIMARY KEY",
        ("asset_integrity_results", "asset_integrity_results_pkey"): "PRIMARY KEY",
        ("asset_integrity_results", "asset_integrity_results_batch_fkey"): "FOREIGN KEY",
        ("publication_gate_attempts", "publication_gate_attempts_pkey"): "PRIMARY KEY",
        ("publication_gate_attempts", "publication_gate_attempts_candidate_fkey"): "FOREIGN KEY",
        ("publication_gate_attempts", "publication_gate_attempts_pipeline_run_fkey"): "FOREIGN KEY",
        (
            "publication_gate_attempts",
            "publication_gate_attempts_integrity_batch_fkey",
        ): "FOREIGN KEY",
        ("publication_integrity_checks", "publication_integrity_checks_pkey"): "PRIMARY KEY",
        (
            "publication_integrity_checks",
            "publication_integrity_checks_publication_fkey",
        ): "FOREIGN KEY",
        ("publication_integrity_checks", "publication_integrity_checks_batch_fkey"): "FOREIGN KEY",
    }
    assert privileges == {
        "asset_integrity_batches": (True, True, False, False),
        "asset_integrity_results": (True, True, False, False),
        "publication_gate_attempts": (True, True, False, False),
        "publication_integrity_checks": (True, True, False, False),
    }


def test_initial_catalog_categories_are_seeded() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                category_slug,
                category_name,
                sort_order,
                is_active
            FROM catalog.dataset_categories
            ORDER BY sort_order
            """
        )

        categories = cursor.fetchall()

    assert categories == [
        ("health-medicine", "Health & Medicine", 10, True),
        ("crime-justice", "Crime & Justice", 20, True),
    ]


def test_contract_snapshots_are_insert_only_for_etl() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                has_table_privilege(
                    'metrka_etl',
                    'meta.contract_snapshots',
                    'SELECT'
                ),
                has_table_privilege(
                    'metrka_etl',
                    'meta.contract_snapshots',
                    'INSERT'
                ),
                has_table_privilege(
                    'metrka_etl',
                    'meta.contract_snapshots',
                    'UPDATE'
                ),
                has_table_privilege(
                    'metrka_etl',
                    'meta.contract_snapshots',
                    'DELETE'
                )
            """
        )

        privileges = cursor.fetchone()

    assert privileges == (True, True, False, False)


def test_contract_snapshots_use_dataset_scoped_primary_key() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT attribute.attname
            FROM pg_index AS index_definition
            CROSS JOIN LATERAL unnest(index_definition.indkey)
                WITH ORDINALITY AS primary_key_column(attnum, position)
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = index_definition.indrelid
             AND attribute.attnum = primary_key_column.attnum
            WHERE index_definition.indrelid =
                  'meta.contract_snapshots'::regclass
              AND index_definition.indisprimary
            ORDER BY primary_key_column.position
            """
        )
        primary_key_columns = cursor.fetchall()

        cursor.execute(
            """
            SELECT is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'meta'
              AND table_name = 'contract_snapshots'
              AND column_name = 'dataset_id'
            """
        )
        dataset_id_nullable = cursor.fetchone()

    assert primary_key_columns == [("dataset_id",), ("contract_hash",)]
    assert dataset_id_nullable == ("NO",)


def test_source_capture_assets_use_path_scoped_primary_key() -> None:
    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT attribute.attname
            FROM pg_index AS index_definition
            CROSS JOIN LATERAL unnest(index_definition.indkey)
                WITH ORDINALITY AS primary_key_column(
                    attnum,
                    position
                )
            JOIN pg_attribute AS attribute
              ON attribute.attrelid =
                 index_definition.indrelid
             AND attribute.attnum =
                 primary_key_column.attnum
            WHERE index_definition.indrelid =
                  'meta.source_capture_assets'::regclass
              AND index_definition.indisprimary
            ORDER BY primary_key_column.position
            """
        )

        primary_key_columns = cursor.fetchall()

    assert primary_key_columns == [("source_capture_id",), ("stream_name",), ("relative_path",)]


def test_source_capture_store_binds_multiple_paths_for_one_stream() -> None:
    capture_id = f"capture_test_{uuid4().hex}"
    dataset_file_id = f"dataset_file_test_{uuid4().hex}"

    paths = (
        "cid0321__single-year__default__all__2002.xlsx",
        "cid0321__single-year__default__all__2025.xlsx",
    )

    with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO meta.marshaled_files (
                dataset_file_id,
                artifact_role,
                bronze_artifacts
            )
            VALUES (%s, 'data', '[]'::jsonb)
            """,
            (dataset_file_id,),
        )
        cursor.execute(
            """
            INSERT INTO meta.source_captures (
                source_capture_id,
                workspace_name,
                captured_at,
                capture_path
            )
            VALUES (%s, 'fl_healthcharts', now(), %s)
            """,
            (capture_id, f"captures/{capture_id}"),
        )

    assets = tuple(
        SourceCaptureAssetBinding(
            stream_name="county",
            dataset_id="fl_healthcharts.county",
            dataset_file_id=dataset_file_id,
            relative_path=relative_path,
            source_url="manual_upload",
            artifact_role="data",
        )
        for relative_path in paths
    )

    try:
        with PostgresSession(_test_dsn()) as session:
            store = PostgresSourceCaptureStore(session)
            store.bind_assets(source_capture_id=capture_id, assets=assets)

            # Повторная запись должна быть безопасной и не создавать копии.
            store.bind_assets(source_capture_id=capture_id, assets=assets)

        with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT relative_path
                FROM meta.source_capture_assets
                WHERE source_capture_id = %s
                  AND stream_name = 'county'
                ORDER BY relative_path
                """,
                (capture_id,),
            )
            stored_paths = tuple(row[0] for row in cursor.fetchall())

        assert stored_paths == tuple(sorted(paths))
    finally:
        with psycopg.connect(_test_dsn()) as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM meta.source_captures
                WHERE source_capture_id = %s
                """,
                (capture_id,),
            )
            cursor.execute(
                """
                DELETE FROM meta.marshaled_files
                WHERE dataset_file_id = %s
                """,
                (dataset_file_id,),
            )
