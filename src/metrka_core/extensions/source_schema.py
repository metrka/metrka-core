"""Public contracts for source-schema extensions."""

from metrka_core.datasets.source_config import SourceConfig
from metrka_core.metadata.file_marshal import FileMarshal
from metrka_core.metadata.source_schema import (
    SOURCE_SCHEMA_HASH_ALGORITHM,
    ParsedSourceSchema,
    SourceSchemaField,
    SourceSchemaFieldBinding,
    compare_source_schema_fields,
)
from metrka_core.metadata.source_schema_store import SourceSchemaStore
from metrka_core.pipeline.bronze.models import BronzeBatchResult, BronzeIngestResult
from metrka_core.storage.bronze_store import BronzeArtifactStore

__all__ = [
    "BronzeArtifactStore",
    "BronzeBatchResult",
    "BronzeIngestResult",
    "FileMarshal",
    "ParsedSourceSchema",
    "SOURCE_SCHEMA_HASH_ALGORITHM",
    "SourceConfig",
    "SourceSchemaField",
    "SourceSchemaFieldBinding",
    "SourceSchemaStore",
    "compare_source_schema_fields",
]
