"""add_photo_url_to_sku_channels

Revision ID: a1b2c3d4e5f6
Revises: 3fe1fad53186
Create Date: 2026-03-15 23:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '3fe1fad53186'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('sku_channels', sa.Column('photo_url', sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column('sku_channels', 'photo_url')
