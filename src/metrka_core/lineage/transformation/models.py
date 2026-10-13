"""Models describing the observed impact of data transformations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

from metrka_core.storage.checksums import parse_sha256_hex


@dataclass(frozen=True)
class TransformationDetailRow:
    """One affected source row and its selected context."""

    source_row_number: int
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.source_row_number < 1:
            raise ValueError("source_row_number must be at least 1")

        invalid_context_names = [
            name for name in self.context if not isinstance(name, str) or not name.strip()
        ]

        if invalid_context_names:
            raise ValueError("Transformation detail context names must be non-empty strings")


class TransformationEvidenceKind(StrEnum):
    """Kinds of preparation evidence recorded by Silver operations."""

    VALUE_CHANGE = "value_change"
    MISSING_NORMALIZATION = "missing_normalization"
    TYPE_CAST = "type_cast"
    DATE_PARSE = "date_parse"
    CASE_CONVERSION = "case_conversion"
    COLUMN_RENAME = "column_rename"
    PARENT_CHILD_RECONCILIATION = "parent_child_reconciliation"


class TransformationEvidenceStatus(StrEnum):
    """Outcome of one configured transformation operation."""

    APPLIED = "applied"
    NO_CHANGE = "no_change"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class TransformationObservation:
    """
    Context-free summary produced by a transformation operation.

    Pipeline identity is added later by the Silver builder.
    """

    operation: str
    column_name: str
    before_value: Any
    after_value: Any
    affected_row_count: int
    record_details: bool = False
    detail_columns: tuple[str, ...] = ()
    detail_rows: tuple[TransformationDetailRow, ...] = ()

    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.operation.strip():
            raise ValueError("operation must not be empty")

        if not self.column_name.strip():
            raise ValueError("column_name must not be empty")

        if self.affected_row_count < 0:
            raise ValueError("affected_row_count must not be negative")

        if not isinstance(self.record_details, bool):
            raise TypeError("record_details must be a boolean")

        invalid_detail_columns = [
            name for name in self.detail_columns if not isinstance(name, str) or not name.strip()
        ]

        if invalid_detail_columns:
            raise ValueError("detail_columns must contain non-empty strings")

        if len(self.detail_columns) != len(set(self.detail_columns)):
            raise ValueError("detail_columns must not contain duplicates")

        if self.record_details:
            if len(self.detail_rows) != self.affected_row_count:
                raise ValueError(
                    "detail_rows count must equal affected_row_count when record_details is enabled"
                )

            expected_context = set(self.detail_columns)

            for detail in self.detail_rows:
                actual_context = set(detail.context)
                missing = sorted(expected_context - actual_context)
                unexpected = sorted(actual_context - expected_context)

                if missing or unexpected:
                    raise ValueError(
                        "Transformation detail row context does not match detail_columns: "
                        f"missing={missing}, unexpected={unexpected}"
                    )
        elif self.detail_columns or self.detail_rows:
            raise ValueError(
                "detail_columns and detail_rows must be empty when record_details is disabled"
            )


@dataclass(frozen=True)
class AutomaticColumnEvidence:
    """
    Evidence produced by a mechanical column transformation.

    Unlike TransformationObservation, this does not describe one
    explicit before-to-after value mapping. It records the aggregate
    outcome of casting, parsing, missing-value normalization or case
    conversion.
    """

    operation: str
    kind: TransformationEvidenceKind
    status: TransformationEvidenceStatus

    column_name: str
    affected_row_count: int
    reason: str

    metrics: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.operation.strip():
            raise ValueError("operation must not be empty")

        if not self.column_name.strip():
            raise ValueError("column_name must not be empty")

        if self.affected_row_count < 0:
            raise ValueError("affected_row_count must not be negative")

        if not self.reason.strip():
            raise ValueError("reason must not be empty")

    @property
    def before_value(self) -> None:
        """Automatic aggregate evidence has no single source value."""

        return None

    @property
    def after_value(self) -> None:
        """Automatic aggregate evidence has no single published value."""

        return None

    @property
    def record_details(self) -> bool:
        """Row-level details are not recorded in the first version."""

        return False

    @property
    def detail_rows(self) -> tuple[TransformationDetailRow, ...]:
        """Automatic evidence currently contains summary metrics only."""

        return ()

    @property
    def meta(self) -> dict[str, Any]:
        """Serialize automatic evidence into the existing impact store."""

        return {
            "evidence_kind": self.kind.value,
            "evidence_status": self.status.value,
            "reason": self.reason,
            "metrics": self.metrics,
        }


type TransformationEvidence = TransformationObservation | AutomaticColumnEvidence


@dataclass(frozen=True)
class TransformationImpact:
    """
    Aggregated evidence of one transformation applied to one field.

    One record represents one before-to-after value change, not every
    individual affected source row.
    """

    pipeline_run_id: str
    dataset_id: str
    dataset_file_id: str
    bronze_run_id: str
    silver_run_id: str
    silver_build_id: str

    table_key: str
    operation: str
    column_name: str

    before_value: Any
    after_value: Any
    affected_row_count: int
    transformation_impact_id: str
    recorded_at: datetime

    partition_key: str | None = None
    partition_value: str | None = None
    version_period: date | None = None
    contract_hash: str | None = None

    details_path: str | None = None
    details_hash: str | None = None
    details_row_count: int | None = None

    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        required_values = {
            "pipeline_run_id": self.pipeline_run_id,
            "dataset_id": self.dataset_id,
            "dataset_file_id": self.dataset_file_id,
            "bronze_run_id": self.bronze_run_id,
            "silver_run_id": self.silver_run_id,
            "silver_build_id": self.silver_build_id,
            "table_key": self.table_key,
            "operation": self.operation,
            "column_name": self.column_name,
            "transformation_impact_id": self.transformation_impact_id,
        }

        for field_name, value in required_values.items():
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")

        if self.recorded_at.utcoffset() != UTC.utcoffset(self.recorded_at):
            raise ValueError("TransformationImpact.recorded_at must be in UTC")

        if self.affected_row_count < 0:
            raise ValueError("affected_row_count must not be negative")

        if self.details_row_count is not None and self.details_row_count < 0:
            raise ValueError("details_row_count must not be negative")

        details_values = (self.details_path, self.details_hash, self.details_row_count)
        if any(value is not None for value in details_values) and not all(
            value is not None for value in details_values
        ):
            raise ValueError(
                "details_path, details_hash and details_row_count must be set together"
            )

        if self.details_hash is not None:
            try:
                parse_sha256_hex(self.details_hash)
            except ValueError as error:
                raise ValueError(
                    "details_hash must be a 64-character hexadecimal SHA-256 hash"
                ) from error
