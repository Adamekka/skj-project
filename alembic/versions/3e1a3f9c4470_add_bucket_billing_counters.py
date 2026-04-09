"""add bucket billing counters

Revision ID: 3e1a3f9c4470
Revises: 980ba1c57e3d
Create Date: 2026-04-09 23:16:18.199091

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3e1a3f9c4470'
down_revision: Union[str, Sequence[str], None] = '980ba1c57e3d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('buckets', schema=None) as batch_op:
        batch_op.add_column(sa.Column('bandwidth_bytes', sa.Integer(), server_default=sa.text('0'), nullable=False))
        batch_op.add_column(sa.Column('current_storage_bytes', sa.Integer(), server_default=sa.text('0'), nullable=False))
        batch_op.add_column(sa.Column('ingress_bytes', sa.Integer(), server_default=sa.text('0'), nullable=False))
        batch_op.add_column(sa.Column('egress_bytes', sa.Integer(), server_default=sa.text('0'), nullable=False))
        batch_op.add_column(sa.Column('internal_transfer_bytes', sa.Integer(), server_default=sa.text('0'), nullable=False))

    # Storage already existed before billing counters were introduced, so we
    # backfill only the bytes-at-rest total from the current objects table.
    op.execute(
        sa.text(
            """
            UPDATE buckets
            SET current_storage_bytes = COALESCE(
                (SELECT SUM(size) FROM files WHERE files.bucket_id = buckets.id),
                0
            )
            """
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('buckets', schema=None) as batch_op:
        batch_op.drop_column('internal_transfer_bytes')
        batch_op.drop_column('egress_bytes')
        batch_op.drop_column('ingress_bytes')
        batch_op.drop_column('current_storage_bytes')
        batch_op.drop_column('bandwidth_bytes')
