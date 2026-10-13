from __future__ import annotations

from datetime import UTC, datetime
from types import ModuleType
from unittest.mock import MagicMock

import pytest

import metrka_core.pipeline.silver.manage_engine_releases as engine_cli
import metrka_core.pipeline.silver.manage_publication_candidates as publication_cli

FIXED_NOW = datetime(2026, 8, 14, 15, 30, tzinfo=UTC)


class FrozenClock:
    """Return one deterministic administrative timestamp."""

    def now_utc(self) -> datetime:
        return FIXED_NOW


def _patch_session(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType
) -> tuple[MagicMock, MagicMock]:
    session = MagicMock()
    session_manager = MagicMock()
    session_manager.__enter__.return_value = session
    session_manager.__exit__.return_value = False
    session_factory = MagicMock(return_value=session_manager)

    monkeypatch.setattr(module, "resolve_operations_conninfo", lambda: "test-operations-connection")
    monkeypatch.setattr(module, "PostgresSession", session_factory)

    return session, session_factory


@pytest.mark.parametrize(
    ("arguments", "expected_limit"), [(["list"], 50), (["list", "--limit", "125"], 125)]
)
def test_engine_release_list_passes_bounded_limit(
    monkeypatch: pytest.MonkeyPatch, arguments: list[str], expected_limit: int
) -> None:
    _, session_factory = _patch_session(monkeypatch, engine_cli)
    store = MagicMock()
    store.list_releases.return_value = []
    monkeypatch.setattr(engine_cli, "PostgresSilverEngineReleaseStore", lambda _session: store)

    result = engine_cli.main(arguments)

    assert result == 0
    store.list_releases.assert_called_once_with(limit=expected_limit)
    session_factory.assert_called_once_with("test-operations-connection")


@pytest.mark.parametrize("limit", ["0", "-1", "1001", "not-a-number"])
def test_engine_release_list_rejects_invalid_limit(limit: str) -> None:
    with pytest.raises(SystemExit) as error:
        engine_cli.main(["list", "--limit", limit])

    assert error.value.code == 2


def test_engine_approval_uses_injected_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    session, session_factory = _patch_session(monkeypatch, engine_cli)

    store = MagicMock()
    release = MagicMock()
    release.engine_release_id = "engine-release-1"
    store.approve.return_value = release

    monkeypatch.setattr(engine_cli, "PostgresSilverEngineReleaseStore", lambda _session: store)

    result = engine_cli.main(
        ["approve", "engine-release-1", "--approved-by", "reviewer@example.test"],
        clock=FrozenClock(),
    )

    assert result == 0
    store.approve.assert_called_once_with(
        engine_release_id="engine-release-1",
        approved_by="reviewer@example.test",
        approved_at=FIXED_NOW,
    )
    assert session is not None
    session_factory.assert_called_once_with("test-operations-connection")


def test_engine_rejection_uses_injected_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    _, session_factory = _patch_session(monkeypatch, engine_cli)

    store = MagicMock()
    release = MagicMock()
    release.engine_release_id = "engine-release-1"
    store.reject.return_value = release

    monkeypatch.setattr(engine_cli, "PostgresSilverEngineReleaseStore", lambda _session: store)

    result = engine_cli.main(
        [
            "reject",
            "engine-release-1",
            "--rejected-by",
            "reviewer@example.test",
            "--reason",
            "Engine release needs review.",
        ],
        clock=FrozenClock(),
    )

    assert result == 0
    store.reject.assert_called_once_with(
        engine_release_id="engine-release-1",
        rejected_by="reviewer@example.test",
        rejection_reason="Engine release needs review.",
        rejected_at=FIXED_NOW,
    )
    session_factory.assert_called_once_with("test-operations-connection")


def test_publication_candidate_approval_uses_injected_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, session_factory = _patch_session(monkeypatch, publication_cli)

    store = MagicMock()
    candidate = MagicMock()
    candidate.candidate_id = "candidate-1"
    candidate.dataset_id = "example.dataset"
    candidate.partition_value = "2025"
    store.approve.return_value = candidate

    monkeypatch.setattr(
        publication_cli, "PostgresDatasetPublicationCandidateStore", lambda _session: store
    )

    result = publication_cli.main(
        ["approve", "candidate-1", "--approved-by", "reviewer@example.test"], clock=FrozenClock()
    )

    assert result == 0
    store.approve.assert_called_once_with(
        candidate_id="candidate-1", approved_by="reviewer@example.test", approved_at=FIXED_NOW
    )
    session_factory.assert_called_once_with("test-operations-connection")


def test_publication_candidate_rejection_uses_injected_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, session_factory = _patch_session(monkeypatch, publication_cli)

    store = MagicMock()
    candidate = MagicMock()
    candidate.candidate_id = "candidate-1"
    candidate.dataset_id = "example.dataset"
    candidate.partition_value = "2025"
    store.reject.return_value = candidate

    monkeypatch.setattr(
        publication_cli, "PostgresDatasetPublicationCandidateStore", lambda _session: store
    )

    result = publication_cli.main(
        [
            "reject",
            "candidate-1",
            "--rejected-by",
            "reviewer@example.test",
            "--reason",
            "Publication requires correction.",
        ],
        clock=FrozenClock(),
    )

    assert result == 0
    store.reject.assert_called_once_with(
        candidate_id="candidate-1",
        rejected_by="reviewer@example.test",
        rejection_reason="Publication requires correction.",
        rejected_at=FIXED_NOW,
    )
    session_factory.assert_called_once_with("test-operations-connection")
