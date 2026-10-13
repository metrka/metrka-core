from typing import Literal

ArtifactRole = Literal["data", "source_schema", "documentation"]

VALID_ARTIFACT_ROLES: frozenset[str] = frozenset({"data", "source_schema", "documentation"})
