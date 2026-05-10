"""add haystack file locations

Revision ID: a4b7d9c2e8f1
Revises: 6e21b3c2cfcd
Create Date: 2026-05-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a4b7d9c2e8f1"
down_revision: Union[str, Sequence[str], None] = "6e21b3c2cfcd"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("files", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "status",
                sa.String(length=20),
                server_default=sa.text("'ready'"),
                nullable=False,
            )
        )
        batch_op.add_column(sa.Column("volume_id", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("offset", sa.Integer(), nullable=True))
        batch_op.add_column(
            sa.Column(
                "uploaded_internally",
                sa.Boolean(),
                server_default=sa.text("0"),
                nullable=False,
            )
        )
        batch_op.create_index(batch_op.f("ix_files_volume_id"), ["volume_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("files", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_files_volume_id"))
        batch_op.drop_column("uploaded_internally")
        batch_op.drop_column("offset")
        batch_op.drop_column("volume_id")
        batch_op.drop_column("status")
