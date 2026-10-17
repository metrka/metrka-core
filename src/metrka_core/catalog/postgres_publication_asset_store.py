"""PostgreSQL persistence for publication assets."""

from __future__ import annotations

from typing import Any

from metrka_core.catalog.publication_asset_models import (
    DatasetPublicationAsset,
    DatasetPublicationAssetRequest,
)
from metrka_core.metadata.postgres import PostgresSession, to_jsonb

ASSET_COLUMNS = """
    p.publication_id,
    p.dataset_id,
    p.version_period,
    p.revision,
    p.partition_key,
    p.partition_value,
    p.silver_build_id,
    a.table_key,
    a.file_path,
    a.file_format,
    a.row_count,
    a.column_count,
    a.columns_json,
    a.size_bytes,
    a.checksum
"""


def _row_to_asset(row: Any) -> DatasetPublicationAsset:
    record = dict(row)
    columns_json = record["columns_json"]

    if not isinstance(columns_json, list):
        raise ValueError("Publication asset columns_json must be a list")

    return DatasetPublicationAsset(
        publication_id=str(record["publication_id"]),
        dataset_id=str(record["dataset_id"]),
        version_period=record["version_period"],
        revision=int(record["revision"]),
        partition_key=str(record["partition_key"]),
        partition_value=str(record["partition_value"]),
        silver_build_id=str(record["silver_build_id"]),
        table_key=str(record["table_key"]),
        file_path=str(record["file_path"]),
        file_format=str(record["file_format"]),
        row_count=int(record["row_count"]),
        column_count=int(record["column_count"]),
        columns=tuple(str(column) for column in columns_json),
        size_bytes=int(record["size_bytes"]),
        checksum=str(record["checksum"]),
    )


def _request_signature(request: DatasetPublicationAssetRequest) -> tuple[object, ...]:
    return (
        request.file_path,
        request.table_key,
        request.file_format,
        request.row_count,
        request.column_count,
        request.columns,
        request.size_bytes,
        request.checksum,
    )


def _asset_signature(asset: DatasetPublicationAsset) -> tuple[object, ...]:
    return (
        asset.file_path,
        asset.table_key,
        asset.file_format,
        asset.row_count,
        asset.column_count,
        asset.columns,
        asset.size_bytes,
        asset.checksum,
    )


class PostgresDatasetPublicationAssetStore:
    """Persist published Silver table files."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def register(
        self, *, publication_id: str, assets: tuple[DatasetPublicationAssetRequest, ...]
    ) -> tuple[DatasetPublicationAsset, ...]:
        normalized_id = publication_id.strip()

        if not normalized_id:
            raise ValueError("publication_id must not be empty")

        if not assets:
            raise ValueError("A publication must contain at least one asset")

        file_paths = [asset.file_path for asset in assets]

        if len(file_paths) != len(set(file_paths)):
            raise ValueError("Publication assets contain duplicate paths")

        with self._session.transaction(), self._session.cursor() as cursor:
            cursor.execute(
                """
                SELECT publication_id
                FROM catalog.dataset_publications
                WHERE publication_id = %s
                FOR UPDATE
                """,
                (normalized_id,),
            )

            if cursor.fetchone() is None:
                raise RuntimeError(
                    f"Cannot register assets for an unknown publication: {normalized_id}"
                )

            existing = self._select_for_publication(cursor=cursor, publication_id=normalized_id)

            if existing:
                existing_signatures = {_asset_signature(asset) for asset in existing}

                requested_signatures = {_request_signature(asset) for asset in assets}

                if existing_signatures != requested_signatures:
                    raise RuntimeError(
                        "Publication assets are immutable and "
                        "the stored files differ from the "
                        f"requested files: {normalized_id}"
                    )

                return existing

            for asset in assets:
                cursor.execute(
                    """
                    INSERT INTO
                        catalog.dataset_publication_assets (
                            publication_id,
                            file_path,
                            table_key,
                            file_format,
                            row_count,
                            column_count,
                            columns_json,
                            size_bytes,
                            checksum
                        )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    """,
                    (
                        normalized_id,
                        asset.file_path,
                        asset.table_key,
                        asset.file_format,
                        asset.row_count,
                        asset.column_count,
                        to_jsonb(list(asset.columns)),
                        asset.size_bytes,
                        asset.checksum,
                    ),
                )

            registered = self._select_for_publication(cursor=cursor, publication_id=normalized_id)

        if len(registered) != len(assets):
            raise RuntimeError("PostgreSQL returned an unexpected number of publication assets")

        return registered

    def list_for_publication(self, *, publication_id: str) -> tuple[DatasetPublicationAsset, ...]:
        with self._session.cursor() as cursor:
            return self._select_for_publication(
                cursor=cursor, publication_id=publication_id.strip()
            )

    def list_active(self, *, dataset_id: str) -> tuple[DatasetPublicationAsset, ...]:
        with self._session.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    {ASSET_COLUMNS}
                FROM catalog.dataset_publication_assets AS a
                JOIN catalog.dataset_publications AS p
                  ON p.publication_id = a.publication_id
                WHERE p.dataset_id = %s
                  AND p.is_active_revision = true
                ORDER BY
                    p.version_period,
                    p.revision,
                    a.table_key,
                    a.file_path
                """,
                (dataset_id.strip(),),
            )

            return tuple(_row_to_asset(row) for row in cursor.fetchall())

    @staticmethod
    def _select_for_publication(
        *, cursor: Any, publication_id: str
    ) -> tuple[DatasetPublicationAsset, ...]:
        cursor.execute(
            f"""
            SELECT
                {ASSET_COLUMNS}
            FROM catalog.dataset_publication_assets AS a
            JOIN catalog.dataset_publications AS p
              ON p.publication_id = a.publication_id
            WHERE p.publication_id = %s
            ORDER BY
                a.table_key,
                a.file_path
            """,
            (publication_id,),
        )

        return tuple(_row_to_asset(row) for row in cursor.fetchall())
