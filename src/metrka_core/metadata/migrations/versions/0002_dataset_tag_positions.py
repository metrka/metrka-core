"""Keep each dataset's tags in contract order, with the contract's text."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_dataset_tag_positions"
down_revision: str | Sequence[str] | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE catalog.dataset_tag_memberships
            ADD COLUMN tag_position integer,
            ADD COLUMN tag_label text
        """
    )

    # Rows written before this revision get the alphabetical order the catalog has
    # shown so far and no label. A dataset's next Silver build writes its contract order.
    op.execute(
        """
        UPDATE catalog.dataset_tag_memberships AS membership
        SET tag_position = ranked.tag_position
        FROM (
            SELECT
                dataset_id,
                tag_slug,
                row_number() OVER (
                    PARTITION BY dataset_id
                    ORDER BY tag_slug
                ) AS tag_position
            FROM catalog.dataset_tag_memberships
        ) AS ranked
        WHERE membership.dataset_id = ranked.dataset_id
          AND membership.tag_slug = ranked.tag_slug
        """
    )

    op.execute(
        """
        ALTER TABLE catalog.dataset_tag_memberships
            ALTER COLUMN tag_position SET NOT NULL,
            ADD CONSTRAINT dataset_tag_memberships_position_check
                CHECK (tag_position > 0),
            ADD CONSTRAINT dataset_tag_memberships_label_check
                CHECK (tag_label IS NULL OR length(btrim(tag_label)) > 0),
            ADD CONSTRAINT dataset_tag_memberships_position_key
                UNIQUE (dataset_id, tag_position)
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE catalog.dataset_tag_memberships
            DROP CONSTRAINT dataset_tag_memberships_position_key,
            DROP CONSTRAINT dataset_tag_memberships_label_check,
            DROP CONSTRAINT dataset_tag_memberships_position_check,
            DROP COLUMN tag_label,
            DROP COLUMN tag_position
        """
    )
