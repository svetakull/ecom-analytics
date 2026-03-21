"""merge_heads_perf_and_expenses

Revision ID: c3d4e5f6a7b8
Revises: a2b3c4d5e6f7, b5d7f87dc8e0
Create Date: 2026-03-18 22:05:00.000000

"""
from typing import Sequence, Union

revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = ('a2b3c4d5e6f7', 'b5d7f87dc8e0')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
