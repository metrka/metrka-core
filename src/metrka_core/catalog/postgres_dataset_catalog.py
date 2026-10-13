"""PostgreSQL implementation of dataset catalog storage."""

from __future__ import annotations

import re

from metrka_core.metadata.postgres import PostgresSession


def _normalize_tag_slug(value: object) -> str:
    """Normalize a contract tag into a stable slug."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("Silver contract meta.tags must contain non-empty strings")

    tag_slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip()).strip("-")

    if not tag_slug:
        raise ValueError("Silver contract meta.tags must contain valid tag slugs")

    return tag_slug


class PostgresDatasetCatalogStore:
    """Persist dataset categories and tags in PostgreSQL."""

    def __init__(self, session: PostgresSession) -> None:
        self._session = session

    def register_dataset_catalog_from_contract(
        self, *, dataset_id: str, contract_hash: str, contract: dict[str, object]
    ) -> None:
        """Register catalog metadata declared by a Silver contract."""

        meta = contract.get("meta")

        if not isinstance(meta, dict):
            raise ValueError("Silver contract must define a 'meta' mapping")

        category_slug = meta.get("category")

        if not isinstance(category_slug, str) or not category_slug.strip():
            raise ValueError("Silver contract meta.category must be a non-empty category slug")

        category_slug = category_slug.strip()

        tags = meta.get("tags", [])

        if not isinstance(tags, list):
            raise ValueError("Silver contract meta.tags must be a list")

        tag_slugs = sorted({_normalize_tag_slug(tag) for tag in tags})

        with self._session.transaction(), self._session.cursor() as cur:
            cur.execute(
                """
                    SELECT category_slug
                    FROM catalog.dataset_categories
                    WHERE category_slug = %s
                      AND is_active = TRUE
                    LIMIT 1
                    """,
                (category_slug,),
            )

            if cur.fetchone() is None:
                raise ValueError(
                    f"Unknown or inactive dataset category in contract: {category_slug}"
                )

            cur.execute(
                """
                    DELETE FROM catalog.dataset_category_memberships
                    WHERE dataset_id = %s
                      AND source = 'contract'
                      AND category_slug <> %s
                    """,
                (dataset_id, category_slug),
            )

            cur.execute(
                """
                    INSERT INTO catalog.dataset_category_memberships (
                        dataset_id,
                        category_slug,
                        source,
                        contract_hash,
                        updated_at
                    )
                    VALUES (
                        %s,
                        %s,
                        'contract',
                        %s,
                        now()
                    )
                    ON CONFLICT (
                        dataset_id,
                        category_slug
                    ) DO UPDATE SET
                        source = EXCLUDED.source,
                        contract_hash = EXCLUDED.contract_hash,
                        updated_at = now()
                    """,
                (dataset_id, category_slug, contract_hash),
            )

            cur.execute(
                """
                    DELETE FROM catalog.dataset_tag_memberships
                    WHERE dataset_id = %s
                      AND source = 'contract'
                    """,
                (dataset_id,),
            )

            for tag_slug in tag_slugs:
                cur.execute(
                    """
                        INSERT INTO catalog.dataset_tags (
                            tag_slug,
                            updated_at
                        )
                        VALUES (
                            %s,
                            now()
                        )
                        ON CONFLICT (tag_slug) DO UPDATE SET
                            updated_at = now()
                        """,
                    (tag_slug,),
                )

                cur.execute(
                    """
                        INSERT INTO catalog.dataset_tag_memberships (
                            dataset_id,
                            tag_slug,
                            source,
                            contract_hash,
                            updated_at
                        )
                        VALUES (
                            %s,
                            %s,
                            'contract',
                            %s,
                            now()
                        )
                        ON CONFLICT (
                            dataset_id,
                            tag_slug
                        ) DO UPDATE SET
                            source = EXCLUDED.source,
                            contract_hash = EXCLUDED.contract_hash,
                            updated_at = now()
                        """,
                    (dataset_id, tag_slug, contract_hash),
                )
