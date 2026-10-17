"""
Generic Bronze Ingestion Engine.

Workflow:
1. Accepts a file already landed in the Immutable Landing Zone.
2. Hashes the outer file and scans the inner files (Payload Fingerprint).
3. Runs landing → Bronze pre-quality gate.
4. Checks FileMarshal. If the outer hash is known, stops instantly (Idempotent).
5. Records FileMarshal registration result through quality gate.
6. If new, compares the Payload Fingerprint against the previous promoted file.
7. Securely extracts ONLY the inner files that changed to the Bronze run folder.
8. Records Bronze extraction result through quality gate.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from metrka_core.metadata.artifact import ArtifactRole
from metrka_core.metadata.bronze_artifact_integrity import (
    capture_bronze_artifacts,
    require_bronze_artifacts_match_source,
)
from metrka_core.metadata.file_marshal import FileMarshal, get_original_filename
from metrka_core.metadata.file_marshal_errors import DuplicateSourceFileError
from metrka_core.metadata.file_marshal_models import BronzeArtifactDigest, MarshaledFile
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.observability.execution_step_meta import ExecutionStepMeta
from metrka_core.observability.execution_step_scope import run_step
from metrka_core.observability.stores import ExecutionLogStore
from metrka_core.pipeline.bronze.models import BronzeIngestResult
from metrka_core.pipeline.bronze.unpack_zip import ZipExtractResult, secure_extract_zip
from metrka_core.pipeline.runtime_services import (
    BronzeRunIdGenerator,
    Clock,
    DatasetFileIdGenerator,
)
from metrka_core.quality.gates import (
    BronzeOutput,
    LandedFile,
    RunIds,
    check_bronze_output,
    check_landed_file,
)
from metrka_core.quality.models import QualityGateResult, QualityOutputFile
from metrka_core.quality.store import QualityCheckStore
from metrka_core.storage.bronze_store import BronzeArtifactStore
from metrka_core.storage.checksums import sha256_file
from metrka_core.validation.preflight.zip_fingerprint import (
    scan_zip_members,
    zip_members_to_extract,
    zip_members_to_metadata,
)

logger = logging.getLogger(__name__)


def _raise_if_quality_failed(stage: str, quality_result: QualityGateResult) -> None:
    """Raise if a quality gate returned blocking failures."""
    if quality_result.failed:
        raise RuntimeError(
            f"Blocking Bronze quality gate failed during {stage}: {quality_result.error_message}"
        )


def ingest_to_bronze(
    dataset_name: str,
    bronze_store: BronzeArtifactStore,
    marshal: FileMarshal,
    landed_file: Path,
    dataset_id: str,
    source_url: str,
    execution_log_store: ExecutionLogStore,
    quality_store: QualityCheckStore,
    file_marshal_store: PostgresFileMarshalStore,
    *,
    clock: Clock,
    dataset_file_ids: DatasetFileIdGenerator,
    bronze_run_ids: BronzeRunIdGenerator,
    artifact_role: ArtifactRole = "data",
    source_capture_id: str | None = None,
    source_last_modified: datetime | None = None,
    pipeline_run_id: str | None = None,
) -> BronzeIngestResult | None:
    """
    Register and stage a landed file in Bronze.

    Returns the new or existing FileMarshal identity.
    Returns None only when the landed physical file does not exist.
    """

    if not landed_file.exists() or not landed_file.is_file():
        logger.error("Landed file not found: %s", landed_file)
        return None

    original_source_file_name = get_original_filename(landed_file.name) or landed_file.name

    bronze_run_id = bronze_run_ids.new_bronze_run_id()

    with run_step(
        dataset=dataset_name,
        step="ingest_and_stage",
        layer="bronze",
        run_id=bronze_run_id,
        start_meta=lambda execution: ExecutionStepMeta(
            dataset_id=dataset_id,
            source_capture_id=source_capture_id,
            source_file_name=landed_file.name,
            original_source_file_name=original_source_file_name,
            bronze_run_id=execution.run_id,
            extra={
                "landed_file": landed_file.name,
                "source_last_modified": (
                    source_last_modified.isoformat() if source_last_modified is not None else None
                ),
            },
        ),
        execution_log_store=execution_log_store,
    ) as ctx:
        run_id = ctx.execution.run_id
        if run_id is None:
            raise RuntimeError("Execution step did not create a run ID")
        input_files = [landed_file]
        is_zip = landed_file.suffix.lower() == ".zip"

        # ----------------------------------------------------------------
        # 1. Hash source asset and create candidate FileMarshal entity
        # ----------------------------------------------------------------
        content_hash = sha256_file(landed_file)
        size_bytes = landed_file.stat().st_size

        m_file = MarshaledFile(
            dataset_file_id=(dataset_file_ids.new_dataset_file_id()),
            dataset_id=dataset_id,
            source_url=source_url,
            source_file_name=landed_file.name,
            original_source_file_name=original_source_file_name,
            artifact_role=artifact_role,
            source_hash=content_hash,
            file_size=size_bytes,
            ingestion_timestamp=clock.now_utc(),
            source_last_modified=source_last_modified,
            row_count_raw=0,
            column_count_raw=1,
        )

        finish_meta = ExecutionStepMeta(
            dataset_id=dataset_id,
            dataset_file_id=m_file.dataset_file_id,
            source_capture_id=source_capture_id,
            source_file_name=m_file.source_file_name,
            original_source_file_name=m_file.original_source_file_name,
            bronze_run_id=run_id,
            input_file_count=len(input_files),
            input_byte_count=sum(path.stat().st_size for path in input_files),
            extra={
                "source_last_modified": (
                    m_file.source_last_modified.isoformat()
                    if m_file.source_last_modified is not None
                    else None
                )
            },
        )

        # ----------------------------------------------------------------
        # 2. Payload fingerprint
        # ----------------------------------------------------------------
        if is_zip:
            cur_members = scan_zip_members(landed_file)
            fingerprint_meta = zip_members_to_metadata(cur_members)
        else:
            cur_members = None
            fingerprint_meta = {
                landed_file.name: {
                    "name": landed_file.name,
                    "sha256": content_hash,
                    "size": size_bytes,
                }
            }

        # ----------------------------------------------------------------
        # 3. Landing → Bronze pre-quality gate
        # ----------------------------------------------------------------
        quality_ids = RunIds(
            dataset_id=dataset_id,
            run_id=run_id,
            pipeline_run_id=pipeline_run_id,
            dataset_file_id=m_file.dataset_file_id,
        )

        pre_quality = check_landed_file(
            LandedFile(path=landed_file, sha256=content_hash, fingerprint=fingerprint_meta),
            ids=quality_ids,
            store=quality_store,
        )

        if pre_quality.failed:
            ctx.count_blocked(pre_quality.blocked_count)
            ctx.set_finish_meta(
                finish_meta.merged_with(ExecutionStepMeta(extra=pre_quality.to_meta()))
            )
            _raise_if_quality_failed("pre-checks", pre_quality)

        # ----------------------------------------------------------------
        # 4. Register in FileMarshal
        # ----------------------------------------------------------------
        try:
            marshal.register(
                m_file,
                meta={
                    "stage": "landing",
                    "bronze_run_id": run_id,
                    "source_capture_id": source_capture_id,
                    "landing_path": bronze_store.relative_path(landed_file),
                    "payload_fingerprint": fingerprint_meta,
                },
            )
            logger.info("File registered in FileMarshal. Hash: %s", content_hash[:8])

        except DuplicateSourceFileError as exc:
            # Load the already registered file using the same dataset and hash.
            existing = marshal.get_by_hash(exc.dataset_id, exc.source_hash)

            if existing is None:
                raise RuntimeError(
                    "Duplicate hash was detected but the existing "
                    "FileMarshal entry could not be loaded: "
                    f"{exc.dataset_id} {exc.source_hash[:8]}"
                ) from exc

            if not existing.bronze_artifacts:
                raise RuntimeError(
                    "The existing FileMarshal entry has no Bronze artifact manifest. "
                    "The earlier Bronze ingestion did not finish successfully; clear or "
                    "reconcile that disposable entry before retrying: "
                    f"dataset_file_id={existing.file.dataset_file_id}"
                ) from exc

            logger.info(
                "Duplicate file detected. Using existing file %s. Hash: %s",
                existing.file.dataset_file_id,
                content_hash[:8],
            )

            ctx.count_skipped(1)
            ctx.set_finish_meta(
                finish_meta.merged_with(
                    ExecutionStepMeta(
                        # Override the candidate ID with the existing file ID.
                        dataset_file_id=existing.file.dataset_file_id,
                        output_file_count=0,
                        output_byte_count=0,
                        extra=pre_quality.to_meta(),
                    )
                )
            )

            return BronzeIngestResult(
                dataset_file_id=existing.file.dataset_file_id,
                dataset_id=existing.file.dataset_id,
                source_hash=existing.file.source_hash,
                bronze_run_id=existing.bronze_run_id,
                is_new=False,
            )

        # ----------------------------------------------------------------
        # 5. Extract/copy to Bronze
        # ----------------------------------------------------------------
        bronze_run_dir = bronze_store.prepare_run_dir(run_id=run_id)

        output_required = True
        output_paths: list[Path] = []
        expected_source_artifacts: tuple[BronzeArtifactDigest, ...] = ()
        output_meta = ExecutionStepMeta(
            output_file_count=0, output_byte_count=0, extra={"output_files": []}
        )
        extract_result: ZipExtractResult | None = None
        requested_extract_count = 0

        if is_zip:
            if cur_members is None:
                raise RuntimeError(
                    "ZIP ingestion reached extraction without scanned archive members"
                )

            prev_fingerprint_meta = file_marshal_store.get_promoted_fingerprint(dataset_id)

            files_to_extract = zip_members_to_extract(
                current_members=cur_members, previous_metadata=prev_fingerprint_meta
            )

            if not files_to_extract:
                logger.info(
                    "Smart CDC: ZIP container changed, but "
                    "internal files are identical. "
                    "Skipping extraction."
                )
                output_required = False
                ctx.count_skipped(1)

            else:
                expected_source_artifacts = tuple(
                    BronzeArtifactDigest(
                        relative_path=member_name,
                        sha256=cur_members[member_name].sha256,
                        size_bytes=cur_members[member_name].size,
                    )
                    for member_name in sorted(files_to_extract)
                )

                logger.info(
                    "Smart CDC: Extracting %d changed/new files (ignored %d).",
                    len(files_to_extract),
                    len(cur_members) - len(files_to_extract),
                )

                extract_result = secure_extract_zip(
                    zip_path=landed_file,
                    dest_dir=bronze_run_dir,
                    members_to_extract=files_to_extract,
                    safe=True,
                )
                requested_extract_count = len(files_to_extract)

                output_paths = [
                    bronze_run_dir / file_name for file_name in extract_result.extracted_files
                ]

                output_meta = ExecutionStepMeta(
                    output_file_count=extract_result.extracted_count,
                    output_byte_count=sum(
                        path.stat().st_size for path in output_paths if path.is_file()
                    ),
                    extra={
                        "output_files": [bronze_store.relative_path(path) for path in output_paths]
                    },
                )

                ctx.count_success(extract_result.extracted_count)

                logger.info(
                    "Securely extracted %d files to %s",
                    extract_result.extracted_count,
                    bronze_run_dir,
                )

        else:
            bronze_file_name = m_file.original_source_file_name or landed_file.name
            expected_source_artifacts = (
                BronzeArtifactDigest(
                    relative_path=bronze_file_name, sha256=content_hash, size_bytes=size_bytes
                ),
            )

            output_file = bronze_store.copy_into_run(
                run_id=run_id, source_file=landed_file, file_name=bronze_file_name
            )

            output_paths = [output_file]

            output_meta = ExecutionStepMeta(
                output_file_count=1,
                output_byte_count=output_file.stat().st_size,
                extra={"output_files": [bronze_store.relative_path(output_file)]},
            )

            ctx.count_success(1)

            logger.info("Copied flat file to %s", bronze_run_dir)

        # ----------------------------------------------------------------
        # 6. Bronze post-quality gate
        # ----------------------------------------------------------------
        quality_output_files = tuple(
            QualityOutputFile(
                local_path=path, workspace_relative_path=bronze_store.relative_path(path)
            )
            for path in output_paths
        )

        post_quality = check_bronze_output(
            BronzeOutput(
                files=quality_output_files,
                required=output_required,
                extraction=extract_result,
                requested_extract_count=requested_extract_count,
            ),
            ids=quality_ids,
            store=quality_store,
        )

        if post_quality.failed:
            ctx.count_blocked(post_quality.blocked_count)
            ctx.set_finish_meta(
                finish_meta.merged_with(
                    ExecutionStepMeta(extra={**pre_quality.to_meta(), **post_quality.to_meta()})
                ).merged_with(output_meta)
            )
            _raise_if_quality_failed("post_bronze", post_quality)

        if output_required:
            bronze_artifacts = capture_bronze_artifacts(
                bronze_run_dir=bronze_run_dir, output_paths=output_paths
            )
            require_bronze_artifacts_match_source(
                captured=bronze_artifacts, expected_from_source=expected_source_artifacts
            )
            marshal.record_bronze_artifacts(
                m_file.dataset_file_id,
                bronze_artifacts,
                meta={"stage": "bronze", "source_capture_id": source_capture_id},
            )
            output_meta = output_meta.merged_with(
                ExecutionStepMeta(
                    extra={
                        "bronze_artifact_count": len(bronze_artifacts),
                        "bronze_artifact_bytes": sum(
                            artifact.size_bytes for artifact in bronze_artifacts
                        ),
                        "bronze_artifact_hashes_recorded": True,
                    }
                )
            )

        ctx.set_finish_meta(
            finish_meta.merged_with(
                ExecutionStepMeta(extra={**pre_quality.to_meta(), **post_quality.to_meta()})
            ).merged_with(output_meta)
        )

        if not output_required:
            return BronzeIngestResult(
                dataset_file_id=m_file.dataset_file_id,
                dataset_id=m_file.dataset_id,
                source_hash=m_file.source_hash,
                bronze_run_id=run_id,
                is_new=True,
            )

        # ----------------------------------------------------------------
        # 7. Maintain latest pointer
        # ----------------------------------------------------------------
        _update_latest_pointer(
            bronze_store=bronze_store,
            bronze_run_dir=bronze_run_dir,
            dataset_id=dataset_id,
            run_id=run_id,
            content_hash=content_hash,
            updated_at=clock.now_utc(),
        )

    return BronzeIngestResult(
        dataset_file_id=m_file.dataset_file_id,
        dataset_id=m_file.dataset_id,
        source_hash=m_file.source_hash,
        bronze_run_id=run_id,
        is_new=True,
    )


def _update_latest_pointer(
    *,
    bronze_store: BronzeArtifactStore,
    bronze_run_dir: Path,
    dataset_id: str,
    run_id: str,
    content_hash: str,
    updated_at: datetime,
) -> None:
    """Update the current Bronze pointer."""

    logger.info("Updating Bronze latest pointer for stream: %s", dataset_id)

    pointer_data = {
        "dataset_id": dataset_id,
        "layer": "bronze",
        "latest_run_id": run_id,
        "latest_hash": content_hash,
        "updated_at_utc": (updated_at.astimezone(UTC).isoformat()),
        "physical_path": (bronze_store.relative_path(bronze_run_dir)),
    }

    if updated_at.utcoffset() is None:
        raise ValueError("Bronze pointer updated_at must be timezone-aware")

    bronze_store.write_latest_pointer(dataset_id=dataset_id, payload=pointer_data)

    logger.info("Successfully updated Bronze latest pointer for stream: %s", dataset_id)
