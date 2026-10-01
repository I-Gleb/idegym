"""Prepare retention schema on a populated database before rolling out application images."""

import argparse
import os
from pathlib import Path

from sqlalchemy import URL, Connection, create_engine, text
from sqlalchemy.pool import NullPool

UP_STATEMENTS = [
    statement.strip()
    for statement in (Path(__file__).parent / "versions" / "007_up.sql").read_text().split(";")
    if statement.strip()
]
_TERMINAL = (
    "((status)::text = ANY ((ARRAY['SUCCEEDED'::character varying, 'FAILED'::character varying, "
    "'CANCELLED'::character varying, 'FINISHED_BY_WATCHER'::character varying])::text[]))"
)
INDEX_DEFINITIONS = {
    "ix_async_operations_terminal_finished": ("finished_at", f"({_TERMINAL} AND (finished_at IS NOT NULL))"),
    "ix_async_operations_payload_retention": (
        "finished_at",
        f"({_TERMINAL} AND (finished_at IS NOT NULL) AND (payloads_expired_at IS NULL))",
    ),
}
_MIGRATION_LOCK = 42239


def _check_column(connection: Connection) -> None:
    column = connection.execute(
        text(
            "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='async_operations' AND column_name='payloads_expired_at'"
        )
    ).one_or_none()
    if column is None or tuple(column) != ("bigint", "YES", None):
        raise RuntimeError("Retention requires a nullable bigint payloads_expired_at column without a default")


def _index_state(connection: Connection, name: str):
    state = (
        connection.execute(
            text(
                "SELECT c.relkind::text AS relkind, i.indisvalid, i.indisready, i.indislive, "
                "i.indrelid = 'public.async_operations'::regclass AS correct_table, am.amname, "
                "i.indisunique, i.indisexclusion, i.indnatts, i.indnkeyatts, "
                "pg_get_indexdef(i.indexrelid, 1, false) AS first_key, "
                "pg_get_indexdef(i.indexrelid, 2, false) AS second_key, "
                "pg_get_expr(i.indpred, i.indrelid) AS predicate "
                "FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid "
                "LEFT JOIN pg_am am ON am.oid=c.relam WHERE c.oid=to_regclass(:name)"
            ),
            {"name": f"public.{name}"},
        )
        .mappings()
        .one_or_none()
    )
    if state is None:
        return None
    first_key, predicate = INDEX_DEFINITIONS[name]
    expected = {
        "relkind": "i",
        "correct_table": True,
        "amname": "btree",
        "indisunique": False,
        "indisexclusion": False,
        "indnatts": 2,
        "indnkeyatts": 2,
        "first_key": first_key,
        "second_key": "id",
        "predicate": predicate,
    }
    if any(state[key] != value for key, value in expected.items()):
        raise RuntimeError(f"public.{name} has an unexpected definition; inspect it before proceeding")
    return state


def _valid(state) -> bool:
    return state is not None and all(state[key] for key in ("indisvalid", "indisready", "indislive"))


def check_schema(connection: Connection) -> None:
    _check_column(connection)
    for name in INDEX_DEFINITIONS:
        if not _valid(_index_state(connection, name)):
            raise RuntimeError(f"public.{name} is missing or invalid; run operation_retention_schema prepare")


def ensure_schema(connection: Connection, *, allow_populated_build: bool, repair_invalid: bool = False) -> None:
    """Build on an AUTOCOMMIT connection; populated databases require explicit preparation.

    Repair removes only invalid indexes with the expected definition and no active build.
    Existing data is neither backfilled nor expired, and Alembic's version is unchanged.
    """
    populated = connection.scalar(text("SELECT EXISTS (SELECT 1 FROM public.async_operations LIMIT 1)"))
    if populated and not allow_populated_build:
        check_schema(connection)
        return
    connection.execute(text(UP_STATEMENTS[0]))
    _check_column(connection)
    for (name, _), statement in zip(INDEX_DEFINITIONS.items(), UP_STATEMENTS[1:], strict=True):
        state = _index_state(connection, name)
        if _valid(state):
            continue
        if state is not None:
            if not repair_invalid or not allow_populated_build:
                raise RuntimeError(f"public.{name} is invalid; inspect activity, then prepare --repair-invalid")
            if connection.scalar(
                text(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index WHERE index_relid=to_regclass(:name))"
                ),
                {"name": f"public.{name}"},
            ):
                raise RuntimeError(f"public.{name} has an active build; wait for its owner")
            connection.execute(text(f"DROP INDEX CONCURRENTLY public.{name}"))
        connection.execute(text(statement))
        if not _valid(_index_state(connection, name)):
            raise RuntimeError(f"public.{name} is not valid after preparation")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "prepare"))
    parser.add_argument("--repair-invalid", action="store_true")
    parser.add_argument("--statement-timeout-seconds", type=int, default=1800)
    parser.add_argument("--lock-timeout-seconds", type=int, default=2)
    args = parser.parse_args()
    if args.repair_invalid and args.action != "prepare":
        parser.error("--repair-invalid requires prepare")
    if min(args.statement_timeout_seconds, args.lock_timeout_seconds) <= 0:
        parser.error("Timeouts must be positive")
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "idegym"),
    )
    engine = create_engine(url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with engine.connect() as connection:
            for name, value in (
                ("application_name", "idegym-operation-retention-schema"),
                ("lock_timeout", f"{args.lock_timeout_seconds}s"),
                ("statement_timeout", f"{args.statement_timeout_seconds}s"),
            ):
                connection.execute(text("SELECT set_config(:name, :value, false)"), {"name": name, "value": value})
            if args.action == "check":
                check_schema(connection)
            else:
                if not connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": _MIGRATION_LOCK}):
                    raise RuntimeError("Another schema operation holds lock 42239; retry after it completes")
                try:
                    ensure_schema(connection, allow_populated_build=True, repair_invalid=args.repair_invalid)
                finally:
                    connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _MIGRATION_LOCK})
        print("Retention column and indexes are ready; alembic_version and operation payloads are unchanged")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
