"""Tests for publication-aware Silver view writes."""

from __future__ import annotations

from pathlib import Path

from metrka_core.pipeline.silver.silver_artifacts import write_silver_latest_views

from .fakes import FakeSilverArtifactStore, make_manifest


def test_latest_views_are_written_under_the_publication_identity(tmp_path: Path) -> None:
    store = FakeSilverArtifactStore(tmp_path)
    manifest = make_manifest()

    first_paths = write_silver_latest_views(
        silver_store=store,  # type: ignore[arg-type]
        current_manifest=manifest,
        publication_id="publication-1",
    )
    second_paths = write_silver_latest_views(
        silver_store=store,  # type: ignore[arg-type]
        current_manifest=manifest,
        publication_id="publication-2",
    )

    assert first_paths != second_paths
    assert first_paths[0].as_posix().endswith("publication=publication-1/latest.sql")
    assert second_paths[0].as_posix().endswith("publication=publication-2/latest.sql")
    assert [entry[2] for entry in store.written_views] == ["publication-1", "publication-2"]
