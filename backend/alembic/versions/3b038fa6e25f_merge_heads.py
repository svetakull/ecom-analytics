"""merge_heads

Revision ID: 3b038fa6e25f
Revises: 876ea47a58ba, a1b2c3d4e5f6
Create Date: 2026-03-16 09:29:42.849035

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '3b038fa6e25f'
down_revision: Union[str, None] = ('876ea47a58ba', 'a1b2c3d4e5f6')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
