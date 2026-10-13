"""Contract tests for the public Metrka domain terminology."""

from __future__ import annotations

from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_TERMINOLOGY_HEADING = "## Terminology"


def _terminology_section(document_name: str) -> str:
    document = (_REPOSITORY_ROOT / document_name).read_text(encoding="utf-8")

    if document.count(_TERMINOLOGY_HEADING) != 1:
        raise AssertionError(f"{document_name} must contain one Terminology section")

    section = document.split(_TERMINOLOGY_HEADING, maxsplit=1)[1]
    return section.split("\n## ", maxsplit=1)[0].strip()


def test_readme_and_public_api_define_the_same_domain_terms() -> None:
    readme_terminology = _terminology_section("README.md")
    public_api_terminology = _terminology_section("PUBLIC_API.md")

    assert readme_terminology == public_api_terminology


def test_public_terminology_defines_identity_and_ownership_boundaries() -> None:
    terminology = _terminology_section("PUBLIC_API.md")

    required_contracts = (
        "A **workspace** is the configuration and storage boundary",
        "A **stream** is one configured data flow inside a workspace",
        "A **dataset** is",
        "`<workspace_name>.<stream_name>`",
        "`wi_dhs_adult_lead.county`",
        "read them with `parse_dataset_id()`",
        "A **table** is one tabular output produced by a dataset",
        "publications, and published assets belong to a specific dataset",
    )

    for contract in required_contracts:
        assert contract in terminology
