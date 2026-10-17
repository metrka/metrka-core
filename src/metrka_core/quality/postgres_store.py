"""PostgreSQL implementation of the quality-check store."""

from __future__ import annotations

from typing import Any

from metrka_core.metadata.postgres import PostgresSession, to_jsonb


class PostgresQualityCheckStore:
    """Persist quality definitions and results in PostgreSQL."""

    def __init__(self, session: PostgresSession, *, pipeline_run_id: str | None = None) -> None:
        self._session = session
        self._pipeline_run_id = pipeline_run_id

    def upsert_quality_check_definition(self, record: dict[str, Any]) -> None:
        """Insert or update one reusable quality-check definition."""

        with self._session.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quality.quality_check_definitions (
                    check_id,
                    check_name,
                    check_type,
                    layer,
                    target,
                    severity,
                    description,
                    code_ref,
                    default_params,
                    is_active,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    COALESCE(%s, TRUE),
                    now()
                )
                ON CONFLICT (check_id) DO UPDATE SET
                    check_name = EXCLUDED.check_name,
                    check_type = EXCLUDED.check_type,
                    layer = EXCLUDED.layer,
                    target = EXCLUDED.target,
                    severity = EXCLUDED.severity,
                    description = EXCLUDED.description,
                    code_ref = EXCLUDED.code_ref,
                    default_params = EXCLUDED.default_params,
                    is_active = EXCLUDED.is_active,
                    updated_at = now()
                """,
                (
                    record["check_id"],
                    record["check_name"],
                    record["check_type"],
                    record["layer"],
                    record["target"],
                    record["severity"],
                    record.get("description"),
                    record["code_ref"],
                    to_jsonb(record.get("default_params") or {}),
                    record.get("is_active", True),
                ),
            )

    def insert_quality_check_run(self, record: dict[str, Any]) -> None:
        """Insert one executed quality-check result."""

        with self._session.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quality.quality_check_runs (
                    check_id,
                    check_name,
                    check_type,
                    dataset_id,
                    dataset_file_id,
                    silver_build_id,
                    run_id,
                    step_id,
                    layer,
                    target,
                    severity,
                    status,
                    expected,
                    actual,
                    result_summary,
                    details,
                    code_ref,
                    params,
                    duration_ms,
                    pipeline_run_id
                )
                SELECT
                    d.check_id,
                    d.check_name,
                    d.check_type,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    d.layer,
                    d.target,
                    d.severity,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    d.code_ref,
                    %s,
                    %s,
                    %s
                FROM quality.quality_check_definitions d
                WHERE d.check_id = %s
                  AND d.is_active = TRUE
                """,
                (
                    record.get("dataset_id"),
                    record.get("dataset_file_id"),
                    record.get("silver_build_id"),
                    record.get("run_id"),
                    record.get("step_id"),
                    record["status"],
                    to_jsonb(record.get("expected")),
                    to_jsonb(record.get("actual")),
                    record.get("result_summary"),
                    to_jsonb(record.get("details") or {}),
                    to_jsonb(record.get("params") or {}),
                    record.get("duration_ms"),
                    (record.get("pipeline_run_id") or self._pipeline_run_id),
                    record["check_id"],
                ),
            )

            if cur.rowcount != 1:
                raise ValueError(
                    f"Unknown or inactive quality check definition: {record['check_id']}"
                )
