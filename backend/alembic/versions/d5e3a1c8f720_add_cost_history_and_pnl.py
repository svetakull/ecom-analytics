"""add sku_cost_history and pnl_records tables

Revision ID: d5e3a1c8f720
Revises: c4d2f9b01e55
Create Date: 2026-03-14 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa

revision = 'd5e3a1c8f720'
down_revision = 'c4d2f9b01e55'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'sku_cost_history',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('sku_id', sa.Integer(), sa.ForeignKey('skus.id'), nullable=False, index=True),
        sa.Column('effective_from', sa.Date(), nullable=False),
        sa.Column('cost_per_unit', sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column('comment', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_sku_cost_history_sku_id', 'sku_cost_history', ['sku_id'])
    op.create_index('ix_sku_cost_history_period', 'sku_cost_history', ['sku_id', 'effective_from'])

    op.create_table(
        'pnl_records',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('period', sa.String(20), nullable=False, index=True),
        sa.Column('line_item', sa.String(500), nullable=False),
        sa.Column('parent_line', sa.String(500), nullable=True),
        sa.Column('amount', sa.Numeric(precision=18, scale=2), default=0),
        sa.Column('pct_of_revenue', sa.Numeric(precision=8, scale=4), nullable=True),
        sa.Column('sort_order', sa.Integer(), default=0),
        sa.Column('created_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_pnl_records_period', 'pnl_records', ['period'])


def downgrade() -> None:
    op.drop_index('ix_pnl_records_period', 'pnl_records')
    op.drop_table('pnl_records')
    op.drop_index('ix_sku_cost_history_period', 'sku_cost_history')
    op.drop_index('ix_sku_cost_history_sku_id', 'sku_cost_history')
    op.drop_table('sku_cost_history')
