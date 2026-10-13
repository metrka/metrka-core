"""Crash-durable local filesystem writes."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

AtomicFileWriter = Callable[[Path], None]


def atomic_write(destination: Path, writer: AtomicFileWriter) -> Path:
    """
    Write a same-directory temporary file and atomically replace the destination.

    The temporary file is synchronized before replacement. On operating systems
    that support opening directories for synchronization, the parent directory is
    synchronized after replacement as well.
    """

    destination = destination.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _temporary_path(destination)

    try:
        writer(temporary_path)

        if not temporary_path.is_file():
            raise RuntimeError(f"Atomic writer produced no file: {temporary_path}")

        commit_temporary_file(temporary_path=temporary_path, destination=destination)
    finally:
        temporary_path.unlink(missing_ok=True)

    return destination


def atomic_write_bytes(destination: Path, content: bytes) -> Path:
    """Durably replace a file with bytes."""

    def write(temporary_path: Path) -> None:
        temporary_path.write_bytes(content)

    return atomic_write(destination, write)


def atomic_write_text(destination: Path, content: str, *, encoding: str = "utf-8") -> Path:
    """Durably replace a text file."""

    def write(temporary_path: Path) -> None:
        temporary_path.write_text(content, encoding=encoding)

    return atomic_write(destination, write)


def atomic_copy_file(source: Path, destination: Path) -> Path:
    """Durably copy one file without exposing partial destination content."""

    source = source.expanduser().resolve()

    if not source.is_file():
        raise FileNotFoundError(f"Source file does not exist: {source}")

    def copy(temporary_path: Path) -> None:
        shutil.copy2(source, temporary_path)

    return atomic_write(destination, copy)


def commit_temporary_file(*, temporary_path: Path, destination: Path) -> Path:
    """Synchronize and atomically promote an existing same-directory temporary file."""

    temporary_path = temporary_path.expanduser().resolve()
    destination = destination.expanduser().resolve()

    if temporary_path.parent != destination.parent:
        raise ValueError(
            "Atomic replacement requires temporary and destination files to be siblings"
        )

    if not temporary_path.is_file():
        raise FileNotFoundError(f"Temporary file does not exist: {temporary_path}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    _fsync_file(temporary_path)
    os.replace(temporary_path, destination)
    _fsync_directory(destination.parent)

    return destination


def _temporary_path(destination: Path) -> Path:
    suffix = destination.suffix
    stem = destination.name[: -len(suffix)] if suffix else destination.name
    return destination.with_name(f".{stem}.{uuid4().hex}.tmp{suffix}")


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as file_handle:
        os.fsync(file_handle.fileno())


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(directory, flags)

    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
