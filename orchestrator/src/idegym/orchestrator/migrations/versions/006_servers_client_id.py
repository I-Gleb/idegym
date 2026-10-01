"""Index server lookups by client.

Revision ID: 006
Revises: 005
"""

from alembic import op
from idegym.orchestrator.migrations.server_client_index import CREATE_SQL, ensure_index

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        if op.get_context().as_sql:
            op.execute(CREATE_SQL)
        else:
            # Populated databases require the separate prebuild command, outside pod startup.
            ensure_index(op.get_bind(), allow_populated_build=False)


def downgrade() -> None:
    # Retain this additive index during application rollback; see 006_down.sql.
    pass
