"""add_lamoda_to_integration_type_enum

Revision ID: 3fe1fad53186
Revises: 876ea47a58ba
Create Date: 2026-03-15 20:33:12.585182

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '3fe1fad53186'
down_revision: Union[str, None] = 'd5e3a1c8f720'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # PostgreSQL не поддерживает ADD VALUE внутри транзакции — используем COMMIT
    op.execute("ALTER TYPE integrationtype ADD VALUE IF NOT EXISTS 'lamoda'")


def downgrade() -> None:
    # PostgreSQL не позволяет удалять значения из enum — downgrade только документальный
    pass
