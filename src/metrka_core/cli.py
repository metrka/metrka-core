"""Installed command-line interface for Metrka pipeline operations."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from importlib.metadata import version as distribution_version
from pathlib import Path

from metrka_core.api import (
    PipelineBootstrapOptions,
    RuntimeEnvironment,
    WorkspacePlacement,
    export_workspace,
    import_workspace,
    initialize_workspace,
    run_pipeline,
    validate_workspace,
    verify_workspace_export,
)
from metrka_core.operations.cli import main as run_operations
from metrka_core.pipeline.provenance import collect_core_code_revision

logger = logging.getLogger(__name__)

_CORE_DISTRIBUTION_NAME = "metrka-core"


def _version_text() -> str:
    """Return the installed package version bound to its exact source revision."""

    revision, dirty = collect_core_code_revision()
    qualifiers = [f"commit {revision.commit_sha[:12]}"]

    if dirty:
        qualifiers.append("dirty")

    return (
        f"{_CORE_DISTRIBUTION_NAME} "
        f"{distribution_version(_CORE_DISTRIBUTION_NAME)} "
        f"({', '.join(qualifiers)})"
    )


class _VersionAction(argparse.Action):
    """Resolve provenance only when the operator actually requests it."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        del namespace, values, option_string
        print(_version_text())
        parser.exit()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metrka", description="Run and operate Metrka data pipelines."
    )
    parser.add_argument(
        "--version",
        action=_VersionAction,
        nargs=0,
        help="Show the installed package version and source commit, then exit.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Run one configured workspace.",
        description="Run acquisition and every configured action for one workspace.",
    )
    run_parser.add_argument("workspace_name", help="Workspace name from placement configuration.")
    run_parser.add_argument(
        "--date",
        dest="target_date",
        default=None,
        help="Landing date to process in YYYY-MM-DD format.",
    )
    run_parser.add_argument(
        "--dataset-id",
        dest="target_dataset_id",
        default=None,
        help="Limit Silver processing to one complete dataset_id.",
    )
    run_parser.add_argument(
        "--source-capture-id",
        default=None,
        help="Select one source capture inside the landing date. Requires --date.",
    )
    run_parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Rebuild Silver even if the same signature succeeded. Requires --dataset-id.",
    )
    run_parser.add_argument(
        "--config-name",
        default="main.yaml",
        help="Workspace pipeline configuration filename. Default: main.yaml.",
    )
    run_parser.add_argument(
        "--environment",
        type=RuntimeEnvironment,
        choices=list(RuntimeEnvironment),
        default=None,
        help="Runtime environment. Defaults to METRKA_ENV or development.",
    )
    run_parser.add_argument(
        "--workspaces-config-path",
        type=Path,
        default=None,
        help="Path to workspace placement YAML. Defaults to METRKA_WORKSPACES_CONFIG_PATH.",
    )
    run_parser.add_argument(
        "--metadata-config-path",
        type=Path,
        default=None,
        help="Path to metadata database configuration. The DSN is never accepted as a CLI option.",
    )
    run_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and include a traceback when execution fails.",
    )

    subparsers.add_parser(
        "operations",
        help="Administer metadata, governance decisions, and publications.",
        description=(
            "Administer metadata, governance decisions, and publications. "
            "Run 'metrka operations --help' for the available workflows."
        ),
    )

    workspace_parser = subparsers.add_parser(
        "workspace", help="Create and manage local workspaces."
    )
    workspace_subparsers = workspace_parser.add_subparsers(dest="workspace_command", required=True)
    init_parser = workspace_subparsers.add_parser(
        "init",
        help="Create and register a Bronze-ready HTTP workspace.",
        description=(
            "Create a standard workspace configured for HTTP acquisition and "
            "Bronze preservation. Silver contracts must be added explicitly later."
        ),
    )
    init_parser.add_argument(
        "workspace_name", help="Stable lowercase workspace identifier, for example example_dataset."
    )
    init_parser.add_argument(
        "--download-url",
        required=True,
        help="Absolute HTTP or HTTPS URL for the initial source file.",
    )
    init_parser.add_argument(
        "--environment",
        type=RuntimeEnvironment,
        choices=list(RuntimeEnvironment),
        default=None,
        help="Runtime environment. Defaults to METRKA_ENV or development.",
    )
    init_parser.add_argument(
        "--workspaces-config-path",
        type=Path,
        default=None,
        help=(
            "Path to workspace placement YAML. Defaults to METRKA_WORKSPACES_CONFIG_PATH "
            "or workspaces.local.yaml in the current directory."
        ),
    )
    init_parser.add_argument(
        "--placement",
        type=WorkspacePlacement,
        choices=list(WorkspacePlacement),
        default=WorkspacePlacement.PORTABLE,
        help="Physical workspace placement. Default: portable.",
    )
    init_parser.add_argument(
        "--workspace-root",
        type=Path,
        default=None,
        help=(
            "Common root for a portable workspace. Required for portable placement. "
            "Relative paths are resolved from the placement configuration file."
        ),
    )
    init_parser.add_argument(
        "--definition-root",
        type=Path,
        default=None,
        help="Definitions root for managed placement. Required with --placement managed.",
    )
    init_parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Persistent data root for managed placement. Required with --placement managed.",
    )
    init_parser.add_argument(
        "--stream-name", default="data", help="Initial stream identifier. Default: data."
    )
    init_parser.add_argument(
        "--official-filename",
        default=None,
        help="Downloaded filename. Defaults to the last component of --download-url.",
    )
    init_parser.add_argument(
        "--source-name",
        default=None,
        help="Human-readable source name. Defaults to the workspace name.",
    )
    init_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and include a traceback when initialization fails.",
    )

    validate_parser = workspace_subparsers.add_parser(
        "validate",
        help="Validate a workspace without running its pipeline.",
        description=(
            "Validate workspace paths, pipeline configuration, quality gates, "
            "registered components, and configured Silver contracts without "
            "connecting to PostgreSQL or changing workspace state."
        ),
    )
    validate_parser.add_argument(
        "workspace_name", help="Workspace name from placement configuration."
    )
    validate_parser.add_argument(
        "--config-name",
        default="main.yaml",
        help="Workspace pipeline configuration filename. Default: main.yaml.",
    )
    validate_parser.add_argument(
        "--environment",
        type=RuntimeEnvironment,
        choices=list(RuntimeEnvironment),
        default=None,
        help="Runtime environment. Defaults to METRKA_ENV or development.",
    )
    validate_parser.add_argument(
        "--workspaces-config-path",
        type=Path,
        default=None,
        help="Path to workspace placement YAML. Defaults to METRKA_WORKSPACES_CONFIG_PATH.",
    )
    validate_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and include a traceback when validation fails.",
    )

    export_parser = workspace_subparsers.add_parser(
        "export",
        help="Assemble a workspace as a verified portable customer package.",
        description=(
            "Reconstruct a portable workspace ZIP from configured definition and data roots. "
            "Every payload file is recorded by size and SHA-256 in the package manifest."
        ),
    )
    export_parser.add_argument(
        "workspace_name", help="Workspace name from placement configuration."
    )
    export_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Destination .zip path. It must be outside the source workspace.",
    )
    export_parser.add_argument(
        "--environment",
        type=RuntimeEnvironment,
        choices=list(RuntimeEnvironment),
        default=None,
        help="Runtime environment. Defaults to METRKA_ENV or development.",
    )
    export_parser.add_argument(
        "--workspaces-config-path",
        type=Path,
        default=None,
        help="Path to workspace placement YAML. Defaults to METRKA_WORKSPACES_CONFIG_PATH.",
    )
    export_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing export package at the destination.",
    )
    export_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and include a traceback when export fails.",
    )

    verify_export_parser = workspace_subparsers.add_parser(
        "verify-export", help="Verify a customer workspace package and every declared file."
    )
    verify_export_parser.add_argument("package_path", type=Path, help="Workspace export .zip.")
    verify_export_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and include a traceback when verification fails.",
    )

    import_parser = workspace_subparsers.add_parser(
        "import",
        help="Install and register a verified customer workspace package.",
        description=(
            "Verify a customer workspace ZIP, extract it below a new destination directory, "
            "and register the resulting portable workspace. Existing workspaces are never "
            "overwritten."
        ),
    )
    import_parser.add_argument("package_path", type=Path, help="Workspace export .zip.")
    import_parser.add_argument(
        "--destination-directory",
        type=Path,
        required=True,
        help=(
            "Parent directory for the imported workspace. The package workspace name is "
            "appended automatically."
        ),
    )
    import_parser.add_argument(
        "--environment",
        type=RuntimeEnvironment,
        choices=list(RuntimeEnvironment),
        default=None,
        help="Runtime environment. Defaults to METRKA_ENV or development.",
    )
    import_parser.add_argument(
        "--workspaces-config-path",
        type=Path,
        default=None,
        help=(
            "Path to workspace placement YAML. Defaults to METRKA_WORKSPACES_CONFIG_PATH "
            "or workspaces.local.yaml in the current directory."
        ),
    )
    import_parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging and include a traceback when import fails.",
    )

    return parser


def _configure_logging(*, debug: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _run_command(args: argparse.Namespace) -> int:
    bootstrap = PipelineBootstrapOptions(
        config_name=args.config_name,
        runtime_environment=args.environment,
        workspaces_config_path=args.workspaces_config_path,
        metadata_config_path=args.metadata_config_path,
    )

    try:
        result = run_pipeline(
            args.workspace_name,
            target_date=args.target_date,
            target_dataset_id=args.target_dataset_id,
            source_capture_id=args.source_capture_id,
            force_rebuild=args.force_rebuild,
            bootstrap=bootstrap,
        )
    except KeyboardInterrupt:
        logger.error("Pipeline interrupted by the operator")
        return 130
    except Exception as error:
        if args.debug:
            logger.exception("Pipeline execution failed")
        else:
            logger.error("Pipeline execution failed: %s: %s", type(error).__name__, error)

        return 1

    print(result.pipeline_run_id)
    return 0


def _workspace_init_command(args: argparse.Namespace) -> int:
    try:
        result = initialize_workspace(
            args.workspace_name,
            download_url=args.download_url,
            runtime_environment=args.environment,
            workspaces_config_path=args.workspaces_config_path,
            placement=args.placement,
            workspace_root=args.workspace_root,
            definition_root=args.definition_root,
            data_root=args.data_root,
            stream_name=args.stream_name,
            official_filename=args.official_filename,
            source_name=args.source_name,
        )
    except KeyboardInterrupt:
        logger.error("Workspace initialization interrupted by the operator")
        return 130
    except Exception as error:
        if args.debug:
            logger.exception("Workspace initialization failed")
        else:
            logger.error("Workspace initialization failed: %s: %s", type(error).__name__, error)

        return 1

    print(f"Workspace initialized: {result.workspace_name}")
    print(f"Placement: {result.placement.value}")

    if result.workspace_root is not None:
        print(f"Workspace root: {result.workspace_root}")

    print(f"Definition root: {result.definition_root}")
    print(f"Data root: {result.data_root}")
    print(f"Configuration: {result.workspaces_config_path}")
    return 0


def _workspace_validate_command(args: argparse.Namespace) -> int:
    try:
        result = validate_workspace(
            args.workspace_name,
            config_name=args.config_name,
            runtime_environment=args.environment,
            workspaces_config_path=args.workspaces_config_path,
        )
    except KeyboardInterrupt:
        logger.error("Workspace validation interrupted by the operator")
        return 130
    except Exception as error:
        if args.debug:
            logger.exception("Workspace validation failed")
        else:
            logger.error("Workspace validation failed: %s: %s", type(error).__name__, error)

        return 1

    print(f"Workspace valid: {result.workspace_name}")
    if result.workspace_root is not None:
        print(f"Workspace root: {result.workspace_root}")

    print(f"Definition root: {result.definition_root}")
    print(f"Data root: {result.data_root}")
    print(f"Streams: {result.stream_count}")
    print(f"Pipeline actions: {result.action_count}")
    print(f"Quality checks: {result.quality_check_count}")
    print(f"Silver contracts: {result.silver_contract_count}")
    print(f"Silver tables: {', '.join(result.silver_table_names) or '-'}")
    print(f"Glossary terms: {', '.join(result.glossary_terms) or '-'}")
    return 0


def _workspace_export_command(args: argparse.Namespace) -> int:
    try:
        result = export_workspace(
            args.workspace_name,
            args.output,
            runtime_environment=args.environment,
            workspaces_config_path=args.workspaces_config_path,
            overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        logger.error("Workspace export interrupted by the operator")
        return 130
    except Exception as error:
        if args.debug:
            logger.exception("Workspace export failed")
        else:
            logger.error("Workspace export failed: %s: %s", type(error).__name__, error)
        return 1

    print(f"Workspace exported: {result.workspace_name}")
    print(f"Source placement: {result.source_placement.value}")
    print(f"Package: {result.package_path}")
    print(f"Files: {result.file_count}")
    print(f"Bytes: {result.total_size_bytes}")
    print(f"Checksum: {result.package_checksum}")
    return 0


def _workspace_verify_export_command(args: argparse.Namespace) -> int:
    try:
        result = verify_workspace_export(args.package_path)
    except KeyboardInterrupt:
        logger.error("Workspace export verification interrupted by the operator")
        return 130
    except Exception as error:
        if args.debug:
            logger.exception("Workspace export verification failed")
        else:
            logger.error(
                "Workspace export verification failed: %s: %s", type(error).__name__, error
            )
        return 1

    print(f"Workspace export valid: {result.workspace_name}")
    print(f"Source placement: {result.source_placement.value}")
    print(f"Package: {result.package_path}")
    print(f"Files: {result.file_count}")
    print(f"Bytes: {result.total_size_bytes}")
    print(f"Checksum: {result.package_checksum}")
    return 0


def _workspace_import_command(args: argparse.Namespace) -> int:
    try:
        result = import_workspace(
            args.package_path,
            destination_directory=args.destination_directory,
            runtime_environment=args.environment,
            workspaces_config_path=args.workspaces_config_path,
        )
    except KeyboardInterrupt:
        logger.error("Workspace import interrupted by the operator")
        return 130
    except Exception as error:
        if args.debug:
            logger.exception("Workspace import failed")
        else:
            logger.error("Workspace import failed: %s: %s", type(error).__name__, error)
        return 1

    print(f"Workspace imported: {result.workspace_name}")
    print(f"Source placement: {result.source_placement.value}")
    print(f"Workspace root: {result.workspace_root}")
    print(f"Data root: {result.data_root}")
    print(f"Configuration: {result.workspaces_config_path}")
    print(f"Files: {result.file_count}")
    print(f"Bytes: {result.total_size_bytes}")
    print(f"Checksum: {result.package_checksum}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Parse command-line arguments and execute the selected operation."""

    arguments = list(argv) if argv is not None else sys.argv[1:]

    if arguments and arguments[0] == "operations":
        return run_operations(arguments[1:])

    parser = _build_parser()
    args = parser.parse_args(arguments)

    if args.command == "run":
        if args.source_capture_id is not None and args.target_date is None:
            parser.error("--source-capture-id requires --date")

        if args.force_rebuild and args.target_dataset_id is None:
            parser.error("--force-rebuild requires --dataset-id")

        _configure_logging(debug=args.debug)
        return _run_command(args)

    if args.command == "workspace" and args.workspace_command == "init":
        _configure_logging(debug=args.debug)
        return _workspace_init_command(args)

    if args.command == "workspace" and args.workspace_command == "validate":
        _configure_logging(debug=args.debug)
        return _workspace_validate_command(args)

    if args.command == "workspace" and args.workspace_command == "export":
        _configure_logging(debug=args.debug)
        return _workspace_export_command(args)

    if args.command == "workspace" and args.workspace_command == "verify-export":
        _configure_logging(debug=args.debug)
        return _workspace_verify_export_command(args)

    if args.command == "workspace" and args.workspace_command == "import":
        _configure_logging(debug=args.debug)
        return _workspace_import_command(args)

    parser.error(f"Unsupported command: {args.command}")
