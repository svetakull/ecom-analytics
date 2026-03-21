"""add buyout_rate_override to sku_channels

Revision ID: b3f1e8a92c40
Revises: 9a76ea440240
Create Date: 2026-03-14 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'b3f1e8a92c40'
down_revision = '9a76ea440240'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'sku_channels',
        sa.Column('buyout_rate_override', sa.Numeric(precision=5, scale=4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('sku_channels', 'buyout_rate_override')
