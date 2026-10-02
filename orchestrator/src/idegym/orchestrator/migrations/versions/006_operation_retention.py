"""Add completion-based operation retention (revision 006, after 005)."""

from pathlib import Path

from alembic import op
from sqlalchemy import text

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def _indexes_ready() -> bool:
    return op.get_bind().scalar(
        text(
            "SELECT count(*) = 2 FROM pg_index WHERE indisvalid AND indisready AND indislive "
            "AND indexrelid IN (to_regclass('public.ix_async_operations_terminal_finished'), "
            "to_regclass('public.ix_async_operations_payload_retention'))"
        )
    )


def _execute_sql(suffix: str) -> None:
    for statement in Path(__file__).with_name(f"006_{suffix}.sql").read_text().split(";"):
        if statement.strip():
            op.execute(statement)


def upgrade() -> None:
    context = op.get_context()
    with context.autocommit_block():
        if not context.as_sql:
            populated = op.get_bind().scalar(text("SELECT EXISTS (SELECT 1 FROM public.async_operations LIMIT 1)"))
            if populated and not _indexes_ready():
                raise RuntimeError(
                    "Apply 006_up.sql with psql in autocommit mode before upgrading this populated database"
                )
        _execute_sql("up")
        if not context.as_sql and not _indexes_ready():
            raise RuntimeError("Retention indexes are invalid; repair them before upgrading")


def downgrade() -> None:
    if op.get_bind().scalar(
        text("SELECT EXISTS (SELECT 1 FROM public.async_operations WHERE payloads_expired_at IS NOT NULL LIMIT 1)")
    ):
        raise RuntimeError("Expired payloads require a compatible reader; disable expiration instead of downgrading")
    _execute_sql("down")
