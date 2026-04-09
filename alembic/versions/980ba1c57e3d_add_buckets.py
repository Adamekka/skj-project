"""add buckets

Revision ID: 980ba1c57e3d
Revises: 
Create Date: 2026-04-09 23:13:49.896590

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '980ba1c57e3d'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    connection = op.get_bind()
    inspector = sa.inspect(connection)
    legacy_files_table_exists = 'files' in inspector.get_table_names()

    op.create_table('buckets',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('name', sa.String(length=63), nullable=False),
    sa.Column('user_id', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('name')
    )
    with op.batch_alter_table('buckets', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_buckets_user_id'), ['user_id'], unique=False)

    if legacy_files_table_exists:
        with op.batch_alter_table('files', schema=None) as batch_op:
            batch_op.add_column(sa.Column('bucket_id', sa.Integer(), nullable=True))

        distinct_user_ids = [
            row[0]
            for row in connection.execute(
                sa.text("SELECT DISTINCT user_id FROM files ORDER BY user_id")
            )
        ]

        # Existing objects predate buckets, so each user gets a deterministic legacy
        # bucket before we enforce the new NOT NULL foreign key on files.bucket_id.
        for index, user_id in enumerate(distinct_user_ids, start=1):
            result = connection.execute(
                sa.text(
                    "INSERT INTO buckets (name, user_id) VALUES (:name, :user_id)"
                ),
                {"name": f"legacy-bucket-{index}", "user_id": user_id},
            )
            connection.execute(
                sa.text(
                    "UPDATE files SET bucket_id = :bucket_id WHERE user_id = :user_id"
                ),
                {"bucket_id": result.lastrowid, "user_id": user_id},
            )

        with op.batch_alter_table('files', schema=None) as batch_op:
            batch_op.alter_column('bucket_id', existing_type=sa.Integer(), nullable=False)
            batch_op.create_index(batch_op.f('ix_files_bucket_id'), ['bucket_id'], unique=False)
            batch_op.create_foreign_key(
                'fk_files_bucket_id_buckets', 'buckets', ['bucket_id'], ['id']
            )
    else:
        op.create_table('files',
        sa.Column('id', sa.String(), nullable=False),
        sa.Column('user_id', sa.String(), nullable=False),
        sa.Column('bucket_id', sa.Integer(), nullable=False),
        sa.Column('filename', sa.String(), nullable=False),
        sa.Column('path', sa.String(), nullable=False),
        sa.Column('size', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['bucket_id'], ['buckets.id'], name='fk_files_bucket_id_buckets')
        )
        with op.batch_alter_table('files', schema=None) as batch_op:
            batch_op.create_index(batch_op.f('ix_files_id'), ['id'], unique=False)
            batch_op.create_index(batch_op.f('ix_files_user_id'), ['user_id'], unique=False)
            batch_op.create_index(batch_op.f('ix_files_bucket_id'), ['bucket_id'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('files', schema=None) as batch_op:
        batch_op.drop_constraint('fk_files_bucket_id_buckets', type_='foreignkey')
        batch_op.drop_index(batch_op.f('ix_files_bucket_id'))
        batch_op.drop_column('bucket_id')

    with op.batch_alter_table('buckets', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_buckets_user_id'))

    op.drop_table('buckets')
