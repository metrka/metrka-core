"""PostgreSQL persistence for transformation-impact summaries."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from datetime import date, datetime
from typing import Any

from psycopg.types.json import Jsonb

from metrka_core.lineage.transformation.models import TransformationImpact
from metrka_core.metadata.postgres import PostgresSession


class PostgresTransformationImpactStore:
    """Persist aggregated transformation impacts in PostgreSQL."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def insert_many(self, impacts: Iterable[TransformationImpact]) -> list[str]:
        """
        Insert several summaries in one database transaction.

        Either every impact is persisted or the transaction is rolled back.
        """

        impact_list = list(impacts)

        with self._session.transaction(), self._session.cursor() as cursor:
            for impact in impact_list:
                cursor.execute(
                    """
                    INSERT INTO lineage.transformation_impacts (
                        transformation_impact_id,
                        recorded_at,
                        pipeline_run_id,
                        dataset_id,
                        dataset_file_id,
                        bronze_run_id,
                        silver_run_id,
                        silver_build_id,
                        table_key,
                        operation,
                        column_name,
                        before_value,
                        after_value,
                        affected_row_count,
                        partition_key,
                        partition_value,
                        version_period,
                        contract_hash,
                        details_path,
                        details_hash,
                        details_row_count,
                        meta
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (transformation_impact_id)
                    DO NOTHING
                    """,
                    (
                        impact.transformation_impact_id,
                        impact.recorded_at,
                        impact.pipeline_run_id,
                        impact.dataset_id,
                        impact.dataset_file_id,
                        impact.bronze_run_id,
                        impact.silver_run_id,
                        impact.silver_build_id,
                        impact.table_key,
                        impact.operation,
                        impact.column_name,
                        Jsonb(impact.before_value),
                        Jsonb(impact.after_value),
                        impact.affected_row_count,
                        impact.partition_key,
                        impact.partition_value,
                        impact.version_period,
                        impact.contract_hash,
                        impact.details_path,
                        impact.details_hash,
                        impact.details_row_count,
                        Jsonb(impact.meta),
                    ),
                )

        return [impact.transformation_impact_id for impact in impact_list]

    def list_for_builds(
        self, *, silver_build_ids: Collection[str]
    ) -> tuple[TransformationImpact, ...]:
        """Return typed transformation evidence for selected Silver builds."""

        build_ids = tuple(dict.fromkeys(silver_build_ids))
        if not build_ids:
            return ()

        with self._session.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    transformation_impact_id,
                    recorded_at,
                    pipeline_run_id,
                    dataset_id,
                    dataset_file_id,
                    bronze_run_id,
                    silver_run_id,
                    silver_build_id,
                    table_key,
                    operation,
                    column_name,
                    before_value,
                    after_value,
                    affected_row_count,
                    partition_key,
                    partition_value,
                    version_period,
                    contract_hash,
                    details_path,
                    details_hash,
                    details_row_count,
                    meta
                FROM lineage.transformation_impacts
                WHERE silver_build_id = ANY(%s::uuid[])
                ORDER BY silver_build_id, transformation_impact_id
                """,
                (list(build_ids),),
            )

            return tuple(_impact_from_record(row) for row in cursor.fetchall())


def _impact_from_record(row: Mapping[str, Any]) -> TransformationImpact:
    recorded_at = row["recorded_at"]
    if not isinstance(recorded_at, datetime):
        raise TypeError("transformation impact recorded_at must be a datetime")

    version_period = row.get("version_period")
    if version_period is not None and not isinstance(version_period, date):
        raise TypeError("transformation impact version_period must be a date")

    meta = row.get("meta")
    if not isinstance(meta, dict):
        raise TypeError("transformation impact meta must be a JSON object")

    return TransformationImpact(
        transformation_impact_id=str(row["transformation_impact_id"]),
        recorded_at=recorded_at,
        pipeline_run_id=str(row["pipeline_run_id"]),
        dataset_id=str(row["dataset_id"]),
        dataset_file_id=str(row["dataset_file_id"]),
        bronze_run_id=str(row["bronze_run_id"]),
        silver_run_id=str(row["silver_run_id"]),
        silver_build_id=str(row["silver_build_id"]),
        table_key=str(row["table_key"]),
        operation=str(row["operation"]),
        column_name=str(row["column_name"]),
        before_value=row.get("before_value"),
        after_value=row.get("after_value"),
        affected_row_count=int(row["affected_row_count"]),
        partition_key=(str(row["partition_key"]) if row.get("partition_key") is not None else None),
        partition_value=(
            str(row["partition_value"]) if row.get("partition_value") is not None else None
        ),
        version_period=version_period,
        contract_hash=(str(row["contract_hash"]) if row.get("contract_hash") is not None else None),
        details_path=(str(row["details_path"]) if row.get("details_path") is not None else None),
        details_hash=(str(row["details_hash"]) if row.get("details_hash") is not None else None),
        details_row_count=(
            int(row["details_row_count"]) if row.get("details_row_count") is not None else None
        ),
        meta={str(key): value for key, value in meta.items()},
    )
