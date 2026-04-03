"""drop uq_logistics_op constraint

Revision ID: g2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-04-03 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'g2b3c4d5e6f7'
down_revision: Union[str, None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint('uq_logistics_op', 'logistics_operations', type_='unique')


def downgrade() -> None:
    op.create_unique_constraint(
        'uq_logistics_op', 'logistics_operations',
        ['nm_id', 'operation_date', 'operation_type', 'supply_number']
    )
