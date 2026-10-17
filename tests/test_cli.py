"""Behavioural tests for the installed Metrka command."""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

import metrka_core.cli as cli
from metrka_core.datasets.workspace_location import WorkspacePlacement
from metrka_core.pipeline.config import RuntimeEnvironment
from metrka_core.pipeline.run import PipelineBootstrapOptions


@pytest.mark.parametrize(("dirty", "expected_suffix"), [(False, ""), (True, ", dirty")])
def test_version_command_reports_package_and_exact_source_revision(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dirty: bool,
    expected_suffix: str,
) -> None:
    revision = SimpleNamespace(commit_sha="0123456789abcdef" * 3)
    monkeypatch.setattr(cli, "distribution_version", lambda _name: "1.0.0")
    monkeypatch.setattr(cli, "collect_core_code_revision", lambda: (revision, dirty))

    with pytest.raises(SystemExit) as captured:
        cli.main(["--version"])

    assert captured.value.code == 0
    assert capsys.readouterr().out == (
        f"metrka-core 1.0.0 (commit 0123456789ab{expected_suffix})\n"
    )


def test_root_help_advertises_version_without_reading_provenance(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail_if_called() -> None:
        raise AssertionError("root help must not collect code provenance")

    monkeypatch.setattr(cli, "collect_core_code_revision", fail_if_called)

    with pytest.raises(SystemExit) as captured:
        cli.main(["--help"])

    assert captured.value.code == 0
    assert "--version" in capsys.readouterr().out


def test_run_command_forwards_pipeline_and_bootstrap_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, object]] = []

    def fake_run_pipeline(workspace_name: str, **kwargs: object) -> object:
        calls.append({"workspace_name": workspace_name, **kwargs})
        return SimpleNamespace(pipeline_run_id="pipeline-cli-test")

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    exit_code = cli.main(
        [
            "run",
            "example_workspace",
            "--date",
            "2026-08-17",
            "--dataset-id",
            "example_workspace.county",
            "--source-capture-id",
            "capture-test",
            "--force-rebuild",
            "--config-name",
            "scheduled.yaml",
            "--environment",
            "production",
            "--workspaces-config-path",
            "C:/metrka/workspaces.local.yaml",
            "--metadata-config-path",
            "C:/metrka/metadata.yaml",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == "pipeline-cli-test\n"
    assert len(calls) == 1

    call = calls[0]
    assert call["workspace_name"] == "example_workspace"
    assert call["target_date"] == "2026-08-17"
    assert call["target_dataset_id"] == "example_workspace.county"
    assert call["source_capture_id"] == "capture-test"
    assert call["force_rebuild"] is True

    bootstrap = call["bootstrap"]
    assert isinstance(bootstrap, PipelineBootstrapOptions)
    assert bootstrap.config_name == "scheduled.yaml"
    assert bootstrap.runtime_environment is RuntimeEnvironment.PRODUCTION
    assert bootstrap.workspaces_config_path == Path("C:/metrka/workspaces.local.yaml")
    assert bootstrap.metadata_config_path == Path("C:/metrka/metadata.yaml")
    assert bootstrap.metadata_conninfo is None


def test_run_command_uses_public_runner_defaults(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, object]] = []

    def fake_run_pipeline(workspace_name: str, **kwargs: object) -> object:
        calls.append({"workspace_name": workspace_name, **kwargs})
        return SimpleNamespace(pipeline_run_id="pipeline-defaults")

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)

    assert cli.main(["run", "example_workspace"]) == 0
    assert capsys.readouterr().out == "pipeline-defaults\n"
    assert len(calls) == 1

    call = calls[0]
    assert call["workspace_name"] == "example_workspace"
    assert call["target_date"] is None
    assert call["target_dataset_id"] is None
    assert call["source_capture_id"] is None
    assert call["force_rebuild"] is False

    bootstrap = call["bootstrap"]
    assert isinstance(bootstrap, PipelineBootstrapOptions)
    assert bootstrap == PipelineBootstrapOptions()


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (
            ["run", "example_workspace", "--source-capture-id", "capture-test"],
            "--source-capture-id requires --date",
        ),
        (["run", "example_workspace", "--force-rebuild"], "--force-rebuild requires --dataset-id"),
    ],
)
def test_run_command_rejects_invalid_option_combinations_before_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    message: str,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(cli, "run_pipeline", lambda *args, **kwargs: calls.append((args, kwargs)))

    with pytest.raises(SystemExit) as captured:
        cli.main(argv)

    assert captured.value.code == 2
    assert message in capsys.readouterr().err
    assert calls == []


def test_run_command_reports_pipeline_failure_without_hiding_exit_status(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fail_run_pipeline(_workspace_name: str, **_kwargs: object) -> None:
        raise RuntimeError("Silver failed")

    monkeypatch.setattr(cli, "run_pipeline", fail_run_pipeline)
    caplog.set_level(logging.ERROR)

    assert cli.main(["run", "example_workspace"]) == 1
    assert "Pipeline execution failed: RuntimeError: Silver failed" in caplog.text


def test_run_command_uses_standard_interrupt_exit_code(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def interrupt_pipeline(_workspace_name: str, **_kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "run_pipeline", interrupt_pipeline)
    caplog.set_level(logging.ERROR)

    assert cli.main(["run", "example_workspace"]) == 130
    assert "Pipeline interrupted by the operator" in caplog.text


def test_workspace_init_forwards_scaffolding_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, object]] = []
    workspace_root = Path("workspaces/health/example_dataset")
    config_path = Path("config/workspaces.yaml")

    def fake_initialize_workspace(workspace_name: str, **kwargs: object) -> object:
        calls.append({"workspace_name": workspace_name, **kwargs})
        return SimpleNamespace(
            workspace_name=workspace_name,
            placement=cli.WorkspacePlacement.PORTABLE,
            workspace_root=workspace_root,
            definition_root=workspace_root,
            data_root=workspace_root / "data",
            workspaces_config_path=config_path,
        )

    monkeypatch.setattr(cli, "initialize_workspace", fake_initialize_workspace)

    exit_code = cli.main(
        [
            "workspace",
            "init",
            "example_dataset",
            "--download-url",
            "https://example.org/source.csv",
            "--environment",
            "production",
            "--workspaces-config-path",
            str(config_path),
            "--workspace-root",
            "workspaces/health/example_dataset",
            "--stream-name",
            "county",
            "--official-filename",
            "county.csv",
            "--source-name",
            "Example Department of Health",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == (
        "Workspace initialized: example_dataset\n"
        "Placement: portable\n"
        f"Workspace root: {workspace_root}\n"
        f"Definition root: {workspace_root}\n"
        f"Data root: {workspace_root / 'data'}\n"
        f"Configuration: {config_path}\n"
    )
    assert calls == [
        {
            "workspace_name": "example_dataset",
            "download_url": "https://example.org/source.csv",
            "runtime_environment": RuntimeEnvironment.PRODUCTION,
            "workspaces_config_path": config_path,
            "placement": cli.WorkspacePlacement.PORTABLE,
            "workspace_root": Path("workspaces/health/example_dataset"),
            "definition_root": None,
            "data_root": None,
            "stream_name": "county",
            "official_filename": "county.csv",
            "source_name": "Example Department of Health",
        }
    ]


def test_workspace_init_delegates_environment_resolution_to_public_api(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configured_path = tmp_path / "private" / "workspaces.local.yaml"
    captured_options: list[tuple[object, object]] = []

    def fake_initialize_workspace(_workspace_name: str, **kwargs: object) -> object:
        captured_options.append((kwargs["runtime_environment"], kwargs["workspaces_config_path"]))
        workspace_root = tmp_path / "example_dataset"
        return SimpleNamespace(
            workspace_name="example_dataset",
            placement=cli.WorkspacePlacement.PORTABLE,
            workspace_root=workspace_root,
            definition_root=workspace_root,
            data_root=workspace_root / "data",
            workspaces_config_path=configured_path,
        )

    monkeypatch.setenv("METRKA_WORKSPACES_CONFIG_PATH", str(configured_path))
    monkeypatch.setattr(cli, "initialize_workspace", fake_initialize_workspace)

    assert (
        cli.main(
            [
                "workspace",
                "init",
                "example_dataset",
                "--download-url",
                "https://example.org/source.csv",
                "--workspace-root",
                "datasets/example_dataset",
            ]
        )
        == 0
    )
    assert captured_options == [(None, None)]


def test_workspace_init_reports_validation_failure(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fail_initialize_workspace(_workspace_name: str, **_kwargs: object) -> None:
        raise ValueError("workspace already exists")

    monkeypatch.setattr(cli, "initialize_workspace", fail_initialize_workspace)
    caplog.set_level(logging.ERROR)

    assert (
        cli.main(
            [
                "workspace",
                "init",
                "example_dataset",
                "--download-url",
                "https://example.org/source.csv",
            ]
        )
        == 1
    )
    assert "Workspace initialization failed: ValueError: workspace already exists" in caplog.text


def test_workspace_validate_forwards_static_configuration_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[dict[str, object]] = []
    workspace_root = Path("C:/metrka/datasets/example_dataset")
    definition_root = workspace_root
    data_root = workspace_root / "data"

    def fake_validate_workspace(workspace_name: str, **kwargs: object) -> object:
        calls.append({"workspace_name": workspace_name, **kwargs})
        return SimpleNamespace(
            workspace_name=workspace_name,
            workspace_root=workspace_root,
            definition_root=definition_root,
            data_root=data_root,
            stream_count=2,
            action_count=3,
            quality_check_count=7,
            silver_contract_count=2,
            silver_table_names=("county", "state"),
            glossary_terms=("adult", "unallocated"),
        )

    monkeypatch.setattr(cli, "validate_workspace", fake_validate_workspace)

    exit_code = cli.main(
        [
            "workspace",
            "validate",
            "example_dataset",
            "--config-name",
            "scheduled.yaml",
            "--environment",
            "production",
            "--workspaces-config-path",
            "C:/metrka/workspaces.local.yaml",
        ]
    )

    assert exit_code == 0
    assert calls == [
        {
            "workspace_name": "example_dataset",
            "config_name": "scheduled.yaml",
            "runtime_environment": RuntimeEnvironment.PRODUCTION,
            "workspaces_config_path": Path("C:/metrka/workspaces.local.yaml"),
        }
    ]
    assert capsys.readouterr().out == (
        "Workspace valid: example_dataset\n"
        f"Workspace root: {workspace_root}\n"
        f"Definition root: {definition_root}\n"
        f"Data root: {data_root}\n"
        "Streams: 2\n"
        "Pipeline actions: 3\n"
        "Quality checks: 7\n"
        "Silver contracts: 2\n"
        "Silver tables: county, state\n"
        "Glossary terms: adult, unallocated\n"
    )


def test_workspace_validate_reports_failure_without_a_database_connection(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fail_validate_workspace(_workspace_name: str, **_kwargs: object) -> None:
        raise ValueError("invalid quality config")

    monkeypatch.setattr(cli, "validate_workspace", fail_validate_workspace)
    caplog.set_level(logging.ERROR)

    assert cli.main(["workspace", "validate", "example_dataset"]) == 1
    assert "Workspace validation failed: ValueError: invalid quality config" in caplog.text


def test_workspace_export_forwards_placement_options_and_reports_package(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    calls: list[dict[str, object]] = []
    package_path = (tmp_path / "exports" / "example.zip").resolve()

    def fake_export_workspace(workspace_name: str, destination: Path, **kwargs: object) -> object:
        calls.append({"workspace_name": workspace_name, "destination": destination, **kwargs})
        return SimpleNamespace(
            workspace_name=workspace_name,
            source_placement=WorkspacePlacement.MANAGED,
            package_path=package_path,
            file_count=12,
            total_size_bytes=345,
            package_checksum="sha256:" + "a" * 64,
        )

    monkeypatch.setattr(cli, "export_workspace", fake_export_workspace)

    assert (
        cli.main(
            [
                "workspace",
                "export",
                "example_workspace",
                "--output",
                str(package_path),
                "--environment",
                "production",
                "--workspaces-config-path",
                "C:/metrka/workspaces.yaml",
                "--overwrite",
            ]
        )
        == 0
    )

    assert calls == [
        {
            "workspace_name": "example_workspace",
            "destination": package_path,
            "runtime_environment": RuntimeEnvironment.PRODUCTION,
            "workspaces_config_path": Path("C:/metrka/workspaces.yaml"),
            "overwrite": True,
        }
    ]
    assert capsys.readouterr().out == (
        "Workspace exported: example_workspace\n"
        "Source placement: managed\n"
        f"Package: {package_path}\n"
        "Files: 12\n"
        "Bytes: 345\n"
        f"Checksum: sha256:{'a' * 64}\n"
    )


def test_workspace_verify_export_reports_verified_identity(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    package_path = (tmp_path / "example.zip").resolve()
    calls: list[Path] = []

    def fake_verify_workspace_export(path: Path) -> object:
        calls.append(path)
        return SimpleNamespace(
            workspace_name="example_workspace",
            source_placement=WorkspacePlacement.PORTABLE,
            package_path=package_path,
            file_count=8,
            total_size_bytes=144,
            package_checksum="sha256:" + "b" * 64,
        )

    monkeypatch.setattr(cli, "verify_workspace_export", fake_verify_workspace_export)

    assert cli.main(["workspace", "verify-export", str(package_path)]) == 0
    assert calls == [package_path]
    assert capsys.readouterr().out == (
        "Workspace export valid: example_workspace\n"
        "Source placement: portable\n"
        f"Package: {package_path}\n"
        "Files: 8\n"
        "Bytes: 144\n"
        f"Checksum: sha256:{'b' * 64}\n"
    )


def test_workspace_import_installs_and_registers_verified_package(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    package_path = tmp_path / "packages" / "example_workspace.zip"
    destination_directory = tmp_path / "consumer" / "workspaces"
    config_path = tmp_path / "consumer" / "workspaces.yaml"
    workspace_root = destination_directory / "example_workspace"
    calls: list[dict[str, object]] = []

    def fake_import_workspace(path: Path, **kwargs: object) -> object:
        calls.append({"package_path": path, **kwargs})
        return SimpleNamespace(
            workspace_name="example_workspace",
            source_placement=WorkspacePlacement.MANAGED,
            workspace_root=workspace_root,
            data_root=workspace_root / "data",
            workspaces_config_path=config_path,
            file_count=12,
            total_size_bytes=345,
            package_checksum="sha256:" + "c" * 64,
        )

    monkeypatch.setattr(cli, "import_workspace", fake_import_workspace)

    assert (
        cli.main(
            [
                "workspace",
                "import",
                str(package_path),
                "--destination-directory",
                str(destination_directory),
                "--environment",
                "production",
                "--workspaces-config-path",
                str(config_path),
            ]
        )
        == 0
    )

    assert calls == [
        {
            "package_path": package_path,
            "destination_directory": destination_directory,
            "runtime_environment": RuntimeEnvironment.PRODUCTION,
            "workspaces_config_path": config_path,
        }
    ]
    assert capsys.readouterr().out == (
        "Workspace imported: example_workspace\n"
        "Source placement: managed\n"
        f"Workspace root: {workspace_root}\n"
        f"Data root: {workspace_root / 'data'}\n"
        f"Configuration: {config_path}\n"
        "Files: 12\n"
        "Bytes: 345\n"
        f"Checksum: sha256:{'c' * 64}\n"
    )


def test_workspace_import_reports_failure(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fail_import(*_args: object, **_kwargs: object) -> None:
        raise ValueError("checksum mismatch")

    monkeypatch.setattr(cli, "import_workspace", fail_import)
    caplog.set_level(logging.ERROR)

    assert (
        cli.main(["workspace", "import", "example.zip", "--destination-directory", "workspaces"])
        == 1
    )
    assert "Workspace import failed: ValueError: checksum mismatch" in caplog.text


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (
            ["workspace", "export", "example", "--output", "example.zip"],
            "Workspace export failed: RuntimeError: data root unavailable",
        ),
        (
            ["workspace", "verify-export", "example.zip"],
            "Workspace export verification failed: ValueError: checksum mismatch",
        ),
    ],
)
def test_workspace_export_commands_report_failure(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, argv: list[str], message: str
) -> None:
    def fail_export(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("data root unavailable")

    def fail_verification(*_args: object, **_kwargs: object) -> None:
        raise ValueError("checksum mismatch")

    monkeypatch.setattr(cli, "export_workspace", fail_export)
    monkeypatch.setattr(cli, "verify_workspace_export", fail_verification)
    caplog.set_level(logging.ERROR)

    assert cli.main(argv) == 1
    assert message in caplog.text


def test_operations_command_delegates_to_stable_operator_router(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run_operations(arguments: list[str]) -> int:
        calls.append(arguments)
        return 19

    monkeypatch.setattr(cli, "run_operations", fake_run_operations)

    assert cli.main(["operations", "metadata", "check"]) == 19
    assert calls == [["metadata", "check"]]


def test_pyproject_installs_the_metrka_command() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pyproject = tomllib.loads((project_root / "pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["scripts"] == {"metrka": "metrka_core.cli:main"}
