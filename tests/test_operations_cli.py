"""Contract tests for the installed Metrka operator command group."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from metrka_core.operations import cli


@pytest.mark.parametrize(
    "command", ["metadata", "engine-releases", "publication-candidates", "reconcile-publications"]
)
def test_operations_command_forwards_arguments_to_selected_workflow(
    monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    calls: list[list[str]] = []

    def handler(arguments: Sequence[str]) -> int:
        calls.append(list(arguments))
        return 17

    monkeypatch.setitem(cli._COMMAND_HANDLERS, command, handler)

    assert cli.main([command, "example", "--flag"]) == 17
    assert calls == [["example", "--flag"]]


def test_operations_help_lists_every_stable_workflow(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as captured:
        cli.main(["--help"])

    assert captured.value.code == 0
    output = capsys.readouterr().out

    for command in cli._COMMAND_HANDLERS:
        assert command in output


def test_nested_operation_help_uses_installed_command_path(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as captured:
        cli.main(["engine-releases", "--help"])

    assert captured.value.code == 0
    assert "usage: metrka operations engine-releases" in capsys.readouterr().out
