"""add_perf_client_id_to_integrations

Revision ID: a2b3c4d5e6f7
Revises: 6097ed76f0a0
Create Date: 2026-03-18 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a2b3c4d5e6f7'
down_revision: Union[str, None] = '6097ed76f0a0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('integrations', sa.Column('perf_client_id', sa.String(200), nullable=True))


def downgrade() -> None:
    op.drop_column('integrations', 'perf_client_id')
