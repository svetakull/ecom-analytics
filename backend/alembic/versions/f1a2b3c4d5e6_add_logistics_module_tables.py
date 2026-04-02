"""add logistics module tables

Revision ID: f1a2b3c4d5e6
Revises: 7386daea6621
Create Date: 2026-04-02 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, None] = '7386daea6621'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'ktr_history',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('date_from', sa.Date(), nullable=False),
        sa.Column('date_to', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(5, 2), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
    )

    op.create_table(
        'irp_history',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('date_from', sa.Date(), nullable=False),
        sa.Column('date_to', sa.Date(), nullable=False),
        sa.Column('value', sa.Numeric(5, 2), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.func.now()),
    )

    op.create_table(
        'wb_nomenclature_dims',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('sku_id', sa.Integer(), sa.ForeignKey('skus.id'), nullable=True),
        sa.Column('nm_id', sa.Integer(), nullable=False, unique=True),
        sa.Column('length_cm', sa.Numeric(10, 2), server_default='0'),
        sa.Column('width_cm', sa.Numeric(10, 2), server_default='0'),
        sa.Column('height_cm', sa.Numeric(10, 2), server_default='0'),
        sa.Column('volume_liters', sa.Numeric(10, 4), server_default='0'),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.func.now()),
    )

    op.create_table(
        'wb_card_dims',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('sku_id', sa.Integer(), sa.ForeignKey('skus.id'), nullable=True),
        sa.Column('nm_id', sa.Integer(), nullable=False, unique=True),
        sa.Column('length_cm', sa.Numeric(10, 2), server_default='0'),
        sa.Column('width_cm', sa.Numeric(10, 2), server_default='0'),
        sa.Column('height_cm', sa.Numeric(10, 2), server_default='0'),
        sa.Column('volume_liters', sa.Numeric(10, 4), server_default='0'),
        sa.Column('card_updated_at', sa.DateTime(), nullable=True),
        sa.Column('fetched_at', sa.DateTime(), server_default=sa.func.now()),
    )

    op.create_table(
        'wb_warehouse_tariffs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('warehouse_name', sa.String(200), nullable=False, unique=True),
        sa.Column('base_first_liter', sa.Numeric(10, 2), nullable=False),
        sa.Column('base_per_liter', sa.Numeric(10, 2), nullable=False),
        sa.Column('fetched_at', sa.DateTime(), server_default=sa.func.now()),
    )

    op.create_table(
        'logistics_operations',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('sku_id', sa.Integer(), sa.ForeignKey('skus.id'), nullable=True),
        sa.Column('nm_id', sa.Integer(), nullable=False),
        sa.Column('seller_article', sa.String(100), nullable=False),
        sa.Column('operation_type', sa.String(100), nullable=False),
        sa.Column('warehouse', sa.String(200), nullable=False, server_default=''),
        sa.Column('supply_number', sa.String(100), nullable=False, server_default=''),
        sa.Column('operation_date', sa.Date(), nullable=False),
        sa.Column('coef_fix_start', sa.Date(), nullable=True),
        sa.Column('coef_fix_end', sa.Date(), nullable=True),
        sa.Column('warehouse_coef', sa.Numeric(5, 3), server_default='1.0'),
        sa.Column('ktr_value', sa.Numeric(5, 2), server_default='1.0'),
        sa.Column('irp_value', sa.Numeric(5, 2), server_default='0'),
        sa.Column('base_first_liter', sa.Numeric(10, 2), server_default='46'),
        sa.Column('base_per_liter', sa.Numeric(10, 2), server_default='14'),
        sa.Column('volume_card_liters', sa.Numeric(10, 4), server_default='0'),
        sa.Column('volume_nomenclature_liters', sa.Numeric(10, 4), server_default='0'),
        sa.Column('calculated_wb_volume', sa.Numeric(10, 4), server_default='0'),
        sa.Column('retail_price', sa.Numeric(12, 2), server_default='0'),
        sa.Column('expected_logistics', sa.Numeric(12, 2), server_default='0'),
        sa.Column('actual_logistics', sa.Numeric(12, 2), server_default='0'),
        sa.Column('difference', sa.Numeric(12, 2), server_default='0'),
        sa.Column('operation_status', sa.String(50), server_default=''),
        sa.Column('dimensions_status', sa.String(50), server_default=''),
        sa.Column('volume_difference', sa.Numeric(10, 4), server_default='0'),
        sa.Column('ktr_needs_check', sa.Boolean(), server_default='false'),
        sa.Column('tariff_missing', sa.Boolean(), server_default='false'),
        sa.Column('report_id', sa.String(100), nullable=True),
        sa.UniqueConstraint('nm_id', 'operation_date', 'operation_type', 'supply_number',
                            name='uq_logistics_op'),
    )


def downgrade() -> None:
    op.drop_table('logistics_operations')
    op.drop_table('wb_warehouse_tariffs')
    op.drop_table('wb_card_dims')
    op.drop_table('wb_nomenclature_dims')
    op.drop_table('irp_history')
    op.drop_table('ktr_history')
