"""add logistics_override to sku_channels

Revision ID: c4d2f9b01e55
Revises: b3f1e8a92c40
Create Date: 2026-03-14 00:30:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'c4d2f9b01e55'
down_revision = 'b3f1e8a92c40'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'sku_channels',
        sa.Column('logistics_override', sa.Numeric(precision=10, scale=2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('sku_channels', 'logistics_override')
