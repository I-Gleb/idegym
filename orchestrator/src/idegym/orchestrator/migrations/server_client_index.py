"""Build and validate the client index without advancing the Alembic revision."""

import argparse
import os
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import URL, Connection, create_engine, text
from sqlalchemy.pool import NullPool

INDEX_NAME = "ix_servers_client_id"
CREATE_SQL = (Path(__file__).parent / "versions" / "006_up.sql").read_text()
_MIGRATION_LOCK = 42239
_INDEX_STATE = text("""
    SELECT c.relkind::text AS relkind, i.indisvalid, i.indisready, i.indislive,
           i.indrelid = to_regclass('public.servers') AS correct_table,
           am.amname, i.indisunique, i.indisexclusion, i.indnatts, i.indnkeyatts,
           pg_get_indexdef(i.indexrelid, 1, false) AS key,
           i.indpred IS NULL AS unfiltered, i.indexprs IS NULL AS plain_column
    FROM pg_class c
    LEFT JOIN pg_index i ON i.indexrelid = c.oid
    LEFT JOIN pg_am am ON am.oid = c.relam
    WHERE c.oid = to_regclass('public.ix_servers_client_id')
""")


def index_state(connection: Connection) -> Mapping[str, Any] | None:
    return connection.execute(_INDEX_STATE).mappings().one_or_none()


def validate_definition(state: Mapping[str, Any]) -> None:
    expected = {
        "relkind": "i",
        "correct_table": True,
        "amname": "btree",
        "indisunique": False,
        "indisexclusion": False,
        "indnatts": 1,
        "indnkeyatts": 1,
        "key": "client_id",
        "unfiltered": True,
        "plain_column": True,
    }
    if any(state[key] != value for key, value in expected.items()):
        raise RuntimeError(f"public.{INDEX_NAME} has an unexpected definition; inspect it before proceeding")


def is_valid(state: Mapping[str, Any]) -> bool:
    return all(state[key] for key in ("indisvalid", "indisready", "indislive"))


def check_index(connection: Connection) -> None:
    state = index_state(connection)
    if state is None:
        raise RuntimeError(f"public.{INDEX_NAME} is missing; run the server_client_index build command before rollout")
    validate_definition(state)
    if not is_valid(state):
        raise RuntimeError(
            f"public.{INDEX_NAME} is invalid; inspect build activity, then run server_client_index build --repair-invalid"
        )


def ensure_index(connection: Connection, *, allow_populated_build: bool, repair_invalid: bool = False) -> None:
    """Create the index on an AUTOCOMMIT connection and verify its definition and validity.

    Populated tables require explicit prebuilding. Repair drops only an invalid
    index with the expected definition; it leaves a valid index in place.
    """
    state = index_state(connection)
    if state is not None:
        validate_definition(state)
        if is_valid(state):
            return
        if not repair_invalid:
            check_index(connection)
        if not allow_populated_build:
            raise RuntimeError("Index repair requires the separate prebuild command")
        connection.execute(text("DROP INDEX CONCURRENTLY public.ix_servers_client_id"))

    if not allow_populated_build and connection.scalar(text("SELECT EXISTS (SELECT 1 FROM public.servers LIMIT 1)")):
        raise RuntimeError("Prebuild ix_servers_client_id with server_client_index build before starting this image")

    connection.execute(text(CREATE_SQL))
    check_index(connection)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "build"))
    parser.add_argument("--repair-invalid", action="store_true")
    parser.add_argument("--lock-timeout-seconds", type=int, default=5)
    parser.add_argument("--statement-timeout-seconds", type=int, default=1800)
    args = parser.parse_args()
    if args.repair_invalid and args.action != "build":
        parser.error("--repair-invalid requires build")
    if min(args.lock_timeout_seconds, args.statement_timeout_seconds) <= 0:
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
                ("application_name", "idegym-server-client-index"),
                ("lock_timeout", f"{args.lock_timeout_seconds}s"),
                ("statement_timeout", f"{args.statement_timeout_seconds}s"),
            ):
                connection.execute(text("SELECT set_config(:name, :value, false)"), {"name": name, "value": value})
            if args.action == "check":
                check_index(connection)
            else:
                if not connection.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": _MIGRATION_LOCK}):
                    raise RuntimeError("Another schema operation holds migration lock 42239; retry after it completes")
                try:
                    ensure_index(connection, allow_populated_build=True, repair_invalid=args.repair_invalid)
                finally:
                    connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _MIGRATION_LOCK})
        print(f"public.{INDEX_NAME} is valid; alembic_version is unchanged")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
