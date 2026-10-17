"""Reuse one main.yaml for many datasets by naming a template and filling in its inputs.

A source often configures many datasets the same way: the same extractor, file layout
and Silver settings, with one number or name changed. A template in
``templates/<name>.yaml`` holds that configuration once::

    version: 1
    inputs:
      cid:
        description: CHARTS indicator number, four digits.
      table:
        description: Silver table whose years give the dataset's period.
        default: ${folder}
    main:
      workspace_name: fl_healthcharts
      streams:
        ${folder}:
          official_filename: "cid${cid}__single-year__default__all__*.xlsx"
          ...

and a dataset's ``main.yaml`` names it::

    uses: charts_yearly_county
    with:
      cid: "0321"

``${name}`` is replaced by the input of that name, in keys and in values. A value that is
one placeholder and nothing else takes the input as given, so a number stays a number.
``${folder}`` is always there: the name of the folder that holds ``conf``, which for a
dataset folder is also its stream name. An input without a default must be given in
``with``; a default may use ``${folder}``.

A main.yaml that uses a template sets only ``uses`` and ``with``: a dataset that needs
something else needs another input or another template. Templates are looked up like
profiles: first beside ``conf``, then one level higher (the source folder above dataset
folders). Every reader sees the filled-in configuration, so changing a template changes
the processing configuration of every dataset that uses it, and their next runs rebuild.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

TEMPLATE_DIRECTORY = "templates"
FOLDER_INPUT = "folder"

_TEMPLATE_NAME = re.compile(r"[a-z][a-z0-9_]*")
_INPUT_NAME = re.compile(r"[a-z][a-z0-9_]*")
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_TEMPLATE_FIELDS = frozenset({"version", "inputs", "main"})
_INPUT_FIELDS = frozenset({"description", "default"})
# What a main.yaml that uses a template may contain; the rest is the template's.
_USING_FIELDS = frozenset({"uses", "with"})


class MainTemplateError(ValueError):
    """Raised when a main.yaml cannot be filled in from its template."""


def load_main_config(path: str | Path) -> dict[str, Any]:
    """Read a workspace's main.yaml, filled in from its template when it names one."""

    path = Path(path)
    document = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    if not isinstance(document, dict):
        raise MainTemplateError(f"{path}: top-level YAML must be a mapping")

    if "uses" not in document:
        return document

    return _fill_template(document, main_path=path)


def template_search_paths(name: str, *, main_path: Path) -> tuple[Path, ...]:
    """Where a main.yaml looks for one template, nearest first."""

    conf = main_path.resolve().parent
    return (
        conf.parent / TEMPLATE_DIRECTORY / f"{name}.yaml",
        conf.parent.parent / TEMPLATE_DIRECTORY / f"{name}.yaml",
    )


def _fill_template(document: Mapping[str, Any], *, main_path: Path) -> dict[str, Any]:
    name = document["uses"]
    if not isinstance(name, str) or _TEMPLATE_NAME.fullmatch(name) is None:
        raise MainTemplateError(
            f"{main_path}: uses {name!r} must name a template such as charts_yearly_county"
        )

    template_path, inputs, main = _load_template(name, main_path=main_path)
    problems: list[str] = []

    extra = sorted(set(document) - _USING_FIELDS)
    if extra:
        problems.append(
            f"{main_path}: {extra} come from template {name!r}; a main.yaml that uses a "
            "template sets only 'uses' and 'with'. Add an input or use another template"
        )

    given = document.get("with") or {}
    if not isinstance(given, Mapping):
        problems.append(f"{main_path}: 'with' must map input names to values")
        given = {}

    unknown = sorted(set(given) - set(inputs))
    if unknown:
        problems.append(
            f"{main_path}: with {unknown} are not inputs of template {name!r}; "
            f"it takes {sorted(inputs)}"
        )

    for input_name, value in given.items():
        if not isinstance(value, str | int | float | bool):
            problems.append(f"{main_path}: input {input_name!r} must be a single value")

    folder = main_path.resolve().parent.parent.name
    values: dict[str, Any] = {FOLDER_INPUT: folder}
    missing: set[str] = set()

    for input_name, spec in inputs.items():
        if input_name in given:
            values[input_name] = given[input_name]
        elif "default" in spec:
            values[input_name] = _fill(spec["default"], {FOLDER_INPUT: folder}, missing)
        else:
            problems.append(f"{main_path}: template {name!r} needs input {input_name!r} in 'with'")

    if problems:
        raise MainTemplateError("Template errors:\n- " + "\n- ".join(problems))

    filled: dict[str, Any] = _fill(main, values, missing)
    if missing:
        raise MainTemplateError(
            f"{template_path}: placeholders {sorted(missing)} are not inputs; declare them "
            f"under 'inputs' (${{{FOLDER_INPUT}}} is the only one given without declaring)"
        )

    return filled


def _load_template(
    name: str, *, main_path: Path
) -> tuple[Path, dict[str, dict[str, Any]], dict[str, Any]]:
    candidates = template_search_paths(name, main_path=main_path)
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        looked = ", ".join(str(candidate) for candidate in candidates)
        raise MainTemplateError(f"{main_path}: template {name!r} was not found; looked in {looked}")

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        raise MainTemplateError(f"{path}: YAML parse error: {error}") from error

    if not isinstance(document, Mapping) or document.get("version") != 1:
        raise MainTemplateError(
            f"{path}: a template must be a mapping with 'version: 1' and 'main'"
        )

    unexpected = sorted(set(document) - _TEMPLATE_FIELDS)
    if unexpected:
        raise MainTemplateError(f"{path}: unsupported template fields {unexpected}")

    main = document.get("main")
    if not isinstance(main, dict) or not main:
        raise MainTemplateError(f"{path}: 'main' must be the main.yaml the template fills in")
    if "uses" in main:
        raise MainTemplateError(f"{path}: a template cannot use another template")

    raw_inputs = document.get("inputs") or {}
    if not isinstance(raw_inputs, Mapping):
        raise MainTemplateError(f"{path}: 'inputs' must map input names to their description")

    inputs: dict[str, dict[str, Any]] = {}
    for input_name, spec in raw_inputs.items():
        if not isinstance(input_name, str) or _INPUT_NAME.fullmatch(input_name) is None:
            raise MainTemplateError(f"{path}: input {input_name!r} must be a name such as cid")
        if input_name == FOLDER_INPUT:
            raise MainTemplateError(f"{path}: input {FOLDER_INPUT!r} is given by Metrka")
        spec = spec or {}
        if not isinstance(spec, Mapping) or set(spec) - _INPUT_FIELDS:
            raise MainTemplateError(
                f"{path}: input {input_name!r} may set only {sorted(_INPUT_FIELDS)}"
            )
        inputs[input_name] = dict(spec)

    return path, inputs, main


def _fill(value: Any, values: Mapping[str, Any], missing: set[str]) -> Any:
    if isinstance(value, str):
        whole = _PLACEHOLDER.fullmatch(value)
        if whole is not None and whole.group(1) in values:
            return values[whole.group(1)]
        return _fill_text(value, values, missing)

    if isinstance(value, Mapping):
        return {
            _fill_text(key, values, missing) if isinstance(key, str) else key: _fill(
                item, values, missing
            )
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [_fill(item, values, missing) for item in value]

    return value


def _fill_text(text: str, values: Mapping[str, Any], missing: set[str]) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            missing.add(name)
            return match.group(0)
        return str(values[name])

    return _PLACEHOLDER.sub(replace, text)
