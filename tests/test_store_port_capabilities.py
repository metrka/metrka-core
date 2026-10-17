"""Keep stores limited to capabilities requested by production consumers."""

from metrka_core.catalog.publication_store import DatasetPublicationStore
from metrka_core.lineage.transformation.postgres_store import PostgresTransformationImpactStore
from metrka_core.metadata.postgres_file_marshal import PostgresFileMarshalStore
from metrka_core.pipeline.silver.postgres_build_store import PostgresSilverBuildStore
from metrka_core.quality.store import QualityCheckStore


def _declared_methods(port: type[object]) -> frozenset[str]:
    return frozenset(
        name for name, member in vars(port).items() if not name.startswith("_") and callable(member)
    )


def test_file_marshal_store_exposes_only_consumed_capabilities() -> None:
    assert _declared_methods(PostgresFileMarshalStore) == {
        "transaction",
        "upsert_marshaled_file",
        "insert_marshal_event",
        "get_promoted_fingerprint",
        "check_hash_exists",
        "get_marshaled_file_by_hash",
        "get_marshaled_file",
        "get_promoted_for_version_period",
        "get_silver_candidate_files",
    }


def test_transformation_impact_store_exposes_only_consumed_capabilities() -> None:
    assert _declared_methods(PostgresTransformationImpactStore) == {
        "insert_many",
        "list_for_builds",
    }


def test_quality_check_store_exposes_only_consumed_capabilities() -> None:
    assert _declared_methods(QualityCheckStore) == {
        "upsert_quality_check_definition",
        "insert_quality_check_run",
    }


def test_silver_build_store_exposes_only_consumed_capabilities() -> None:
    assert _declared_methods(PostgresSilverBuildStore) == {
        "insert_started",
        "get_by_id",
        "find_by_ids",
        "list_for_dataset",
        "find_successful_by_signatures",
        "find_latest_successful_for_version",
        "find_latest_attempt_for_version",
        "mark_succeeded",
        "mark_failed",
    }


def test_dataset_publication_store_exposes_only_consumed_capabilities() -> None:
    assert _declared_methods(DatasetPublicationStore) == {
        "publish",
        "get_by_id",
        "find_current",
        "find_active",
        "list_active",
        "list_all",
    }
