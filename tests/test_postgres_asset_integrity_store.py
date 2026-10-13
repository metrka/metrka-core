from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from metrka_core.quality.asset_integrity_models import (
    AssetIntegrityBatch,
    AssetIntegrityResult,
    AssetIntegrityStatus,
)
from metrka_core.quality.postgres_asset_integrity_store import PostgresAssetIntegrityEvidenceStore
from metrka_core.quality.publication_integrity_models import (
    PublicationIntegrityBatchLink,
    PublicationIntegrityCheck,
    PublicationIntegrityTrigger,
)

FIXED_NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)


def _batch() -> AssetIntegrityBatch:
    return AssetIntegrityBatch(
        checked_at=FIXED_NOW,
        results=(
            AssetIntegrityResult(
                file_path="data/files/silver/tables/people/data.parquet",
                status=AssetIntegrityStatus.PASSED,
                expected_size_bytes=5,
                actual_size_bytes=5,
                expected_checksum="sha256:expected",
                actual_checksum="sha256:expected",
            ),
        ),
    )


def test_postgres_store_persists_batch_and_results_once() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"integrity_batch_id": 42}

    integrity_batch_id = PostgresAssetIntegrityEvidenceStore(session).insert_batch(_batch())

    assert integrity_batch_id == 42
    assert cursor.execute.call_count == 2
    batch_parameters = cursor.execute.call_args_list[0].args[1]
    result_parameters = cursor.execute.call_args_list[1].args[1]
    assert batch_parameters == (FIXED_NOW,)
    assert result_parameters[0] == 42
    assert result_parameters[1] == "data/files/silver/tables/people/data.parquet"
    assert result_parameters[2] == "passed"
    session.transaction.assert_called_once_with()


def test_postgres_store_links_existing_batch_without_copying_results() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    link = PublicationIntegrityBatchLink(
        publication_id="publication-1",
        trigger=PublicationIntegrityTrigger.PUBLICATION_COMMIT,
        integrity_batch_id=42,
    )

    PostgresAssetIntegrityEvidenceStore(session).link_batch(link)

    cursor.execute.assert_called_once()
    assert cursor.execute.call_args.args[1] == ("publication-1", 42, "publication_commit")


def test_postgres_store_records_reconciliation_check_atomically() -> None:
    session = MagicMock()
    cursor = session.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"integrity_batch_id": 43}
    check = PublicationIntegrityCheck(
        publication_id="publication-1",
        trigger=PublicationIntegrityTrigger.RECONCILIATION,
        batch=_batch(),
    )

    integrity_batch_id = PostgresAssetIntegrityEvidenceStore(session).insert_check(check)

    assert integrity_batch_id == 43
    assert cursor.execute.call_count == 3
    assert cursor.execute.call_args_list[2].args[1] == ("publication-1", 43, "reconciliation")
    session.transaction.assert_called_once_with()
