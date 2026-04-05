"""add gross commission and acquiring to sku_daily_expenses

Revision ID: 89f9c6d9691e
Revises: g2b3c4d5e6f7
Create Date: 2026-04-05 11:43:05.369173

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '89f9c6d9691e'
down_revision: Union[str, None] = 'g2b3c4d5e6f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'sku_daily_expenses',
        sa.Column('sale_commission_gross', sa.Numeric(12, 2), nullable=False, server_default='0'),
    )
    op.add_column(
        'sku_daily_expenses',
        sa.Column('acquiring_gross', sa.Numeric(12, 2), nullable=False, server_default='0'),
    )


def downgrade() -> None:
    op.drop_column('sku_daily_expenses', 'acquiring_gross')
    op.drop_column('sku_daily_expenses', 'sale_commission_gross')
