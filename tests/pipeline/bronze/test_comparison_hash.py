"""Duplicate detection can leave out JSON values that change without the file changing."""

import json
from pathlib import Path

import pytest

from metrka_core.datasets.source_config import load_source_config
from metrka_core.pipeline.bronze.comparison_hash import sha256_json_ignoring


def _write(path: Path, document: object) -> Path:
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_ignored_keys_and_layout_do_not_change_the_hash(tmp_path: Path) -> None:
    first = _write(tmp_path / "a.json", {"name": "x", "viewCount": 1, "owner": {"views": 1}})
    second = tmp_path / "b.json"
    second.write_text(
        '{\n  "owner": {"views": 9},\n  "viewCount": 7,\n  "name": "x"\n}', encoding="utf-8"
    )
    keys = ("viewCount", "owner.views")

    assert sha256_json_ignoring(first, keys) == sha256_json_ignoring(second, keys)


def test_other_values_still_change_the_hash(tmp_path: Path) -> None:
    first = _write(tmp_path / "a.json", {"name": "x", "viewCount": 1})
    second = _write(tmp_path / "b.json", {"name": "y", "viewCount": 1})

    assert sha256_json_ignoring(first, ("viewCount",)) != sha256_json_ignoring(
        second, ("viewCount",)
    )


def test_a_missing_key_changes_nothing(tmp_path: Path) -> None:
    first = _write(tmp_path / "a.json", {"name": "x"})
    second = _write(tmp_path / "b.json", {"name": "x"})

    assert sha256_json_ignoring(first, ("owner.views", "viewCount")) == sha256_json_ignoring(
        second, ()
    )


def test_a_file_that_is_not_json_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "page.json"
    path.write_text("<html></html>", encoding="utf-8")

    with pytest.raises(ValueError, match="is not a JSON document"):
        sha256_json_ignoring(path, ("viewCount",))


def _config(tmp_path: Path, stream: str) -> Path:
    path = tmp_path / "main.yaml"
    path.write_text(f"workspace_name: example\n\nstreams:\n{stream}", encoding="utf-8")
    return path


def test_source_config_loads_ignored_json_keys(tmp_path: Path) -> None:
    path = _config(
        tmp_path,
        """  about:
    official_filename: about.json
    artifact_role: documentation
    duplicate_check:
      ignore_json_keys: [viewCount, owner.displayName]
""",
    )

    stream = load_source_config(path).streams["about"]

    assert stream.ignored_json_keys == ("viewCount", "owner.displayName")
    assert "duplicate_check" not in stream.extra


@pytest.mark.parametrize(
    ("stream", "message"),
    [
        (
            "  about:\n    official_filename: about.json\n    duplicate_check: [viewCount]\n",
            "must be a mapping with ignore_json_keys only",
        ),
        (
            "  about:\n    official_filename: about.json\n    duplicate_check:\n"
            "      ignore_json_keys: [owner.]\n",
            "must list JSON keys",
        ),
        (
            "  about:\n    official_filename: about.json\n    duplicate_check:\n"
            "      ignore_json_keys: [a, a]\n",
            "repeats a key",
        ),
        (
            "  about:\n    official_filename: about.html\n    duplicate_check:\n"
            "      ignore_json_keys: [a]\n",
            "needs a .json official_filename",
        ),
    ],
)
def test_source_config_refuses_invalid_duplicate_check(
    tmp_path: Path, stream: str, message: str
) -> None:
    with pytest.raises(RuntimeError, match=message):
        load_source_config(_config(tmp_path, stream))
