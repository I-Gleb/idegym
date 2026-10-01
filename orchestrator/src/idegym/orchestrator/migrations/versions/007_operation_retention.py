"""Prepare terminal-operation retention metadata and indexes.

Revision ID: 007
Revises: 006
"""

from alembic import op
from idegym.orchestrator.migrations.operation_retention_schema import UP_STATEMENTS, ensure_schema

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        if op.get_context().as_sql:
            for statement in UP_STATEMENTS:
                op.execute(statement)
        else:
            ensure_schema(op.get_bind(), allow_populated_build=False)


def downgrade() -> None:
    # Keep expiration evidence and additive indexes when reverting application code.
    pass
