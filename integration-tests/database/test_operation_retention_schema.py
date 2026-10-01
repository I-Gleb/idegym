"""Exercise online retention preparation and additive migration rollback on PostgreSQL."""

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from idegym.orchestrator.database.models import AsyncOperation
from idegym.orchestrator.database.operation_retention import clean_operation_batch
from idegym.orchestrator.migrations.operation_retention_schema import (
    UP_STATEMENTS,
    check_schema,
    ensure_schema,
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool


@pytest.fixture
def schema_connection(db, db_url):
    engine = create_engine(make_url(db_url).set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as connection:
            yield connection
    finally:
        engine.dispose()


async def test_populated_tables_require_preparation_without_changing_payloads(db, schema_connection):
    row = AsyncOperation(
        request_type="FORWARD_REQUEST", status="SUCCEEDED", request="keep", result="keep", finished_at=1
    )
    db.add(row)
    await db.commit()
    schema_connection.execute(text("DROP INDEX CONCURRENTLY ix_async_operations_payload_retention"))
    try:
        with pytest.raises(RuntimeError, match="missing or invalid"):
            ensure_schema(schema_connection, allow_populated_build=False)
        ensure_schema(schema_connection, allow_populated_build=True)
        check_schema(schema_connection)
        assert schema_connection.execute(
            text("SELECT request, result, payloads_expired_at FROM async_operations")
        ).one() == ("keep", "keep", None)
    finally:
        ensure_schema(schema_connection, allow_populated_build=True)


async def test_interrupted_concurrent_build_requires_explicit_repair(db, db_url, schema_connection):
    schema_connection.execute(text("DROP INDEX CONCURRENTLY ix_async_operations_payload_retention"))
    async_engine = create_async_engine(db_url)
    try:
        async with async_engine.begin() as blocker:
            await blocker.execute(
                text("INSERT INTO async_operations(request_type, status) VALUES ('FORWARD_REQUEST','SCHEDULED')")
            )

            def interrupted_build():
                with create_engine(
                    make_url(db_url).set(drivername="postgresql+psycopg2"),
                    isolation_level="AUTOCOMMIT",
                    poolclass=NullPool,
                ).connect() as connection:
                    connection.execute(text("SET statement_timeout='150ms'"))
                    connection.execute(text(UP_STATEMENTS[2]))

            with pytest.raises(DBAPIError):
                await asyncio.to_thread(interrupted_build)
        assert (
            schema_connection.scalar(
                text(
                    "SELECT indisvalid FROM pg_index WHERE indexrelid='ix_async_operations_payload_retention'::regclass"
                )
            )
            is False
        )
        with pytest.raises(RuntimeError, match="invalid"):
            ensure_schema(schema_connection, allow_populated_build=True)
        ensure_schema(schema_connection, allow_populated_build=True, repair_invalid=True)
        check_schema(schema_connection)
    finally:
        await async_engine.dispose()
        ensure_schema(schema_connection, allow_populated_build=True, repair_invalid=True)


async def test_preparation_rejects_an_index_with_a_different_definition(db, schema_connection):
    schema_connection.execute(text("DROP INDEX CONCURRENTLY ix_async_operations_payload_retention"))
    schema_connection.execute(
        text("CREATE INDEX ix_async_operations_payload_retention ON async_operations(id, finished_at)")
    )
    try:
        with pytest.raises(RuntimeError, match="unexpected definition"):
            ensure_schema(schema_connection, allow_populated_build=True, repair_invalid=True)
    finally:
        schema_connection.execute(text("DROP INDEX CONCURRENTLY ix_async_operations_payload_retention"))
        ensure_schema(schema_connection, allow_populated_build=True)


async def test_generic_prepared_plan_can_use_the_payload_partial_index(db):
    await db.execute(text("SET LOCAL enable_seqscan=off"))
    await db.execute(text("SET LOCAL plan_cache_mode=force_generic_plan"))
    await db.execute(
        text("""
        PREPARE retention_candidates(bigint, integer) AS
        SELECT id FROM async_operations
        WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','FINISHED_BY_WATCHER')
          AND finished_at IS NOT NULL AND payloads_expired_at IS NULL AND finished_at < $1
        ORDER BY finished_at, id LIMIT $2 FOR UPDATE SKIP LOCKED
    """)
    )
    try:
        plan = "\n".join((await db.execute(text("EXPLAIN EXECUTE retention_candidates(1790856000000, 250)"))).scalars())
        assert "ix_async_operations_payload_retention" in plan
        assert "LockRows" in plan
    finally:
        await db.execute(text("DEALLOCATE retention_candidates"))
        await db.rollback()


async def test_005_to_007_migration_and_additive_rollback(db_url):
    database_name = f"idegym_test_retention_migration_{uuid4().hex[:8]}"
    admin_engine = create_async_engine(db_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    url = make_url(db_url).set(database=database_name)
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    ini = Path(__file__).resolve().parents[2] / "orchestrator/src/idegym/orchestrator/alembic.ini"

    def migrate(revision, *, downgrade=False):
        config = Config(str(ini))
        config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
        (command.downgrade if downgrade else command.upgrade)(config, revision)

    try:
        async with admin_engine.connect() as connection:
            await connection.execute(text(f"CREATE DATABASE {database_name}"))
        await asyncio.to_thread(migrate, "005")
        async with engine.connect() as connection:
            await connection.execute(
                text("""
                INSERT INTO async_operations(request_type, status, request, result, scheduled_at, finished_at)
                VALUES ('FORWARD_REQUEST','SUCCEEDED','request','result',1,2),
                       ('FORWARD_REQUEST','IN_PROGRESS','active','active',1,NULL)
            """)
            )
        with pytest.raises(RuntimeError, match="payloads_expired_at"):
            await asyncio.to_thread(migrate, "head")
        async with engine.connect() as connection:
            await connection.run_sync(lambda sync: ensure_schema(sync, allow_populated_build=True))
            assert (await connection.scalar(text("SELECT version_num FROM alembic_version"))) == "006"
            assert (await connection.scalar(text("SELECT result FROM async_operations WHERE id=1"))) == "result"
        await asyncio.to_thread(migrate, "head")
        transactional = create_async_engine(url, poolclass=NullPool)
        try:
            async with async_sessionmaker(transactional)() as session:
                async with session.begin():
                    batch = await clean_operation_batch(
                        session, action="payload", cutoff_ms=100, expired_at_ms=1000, batch_size=2
                    )
                assert batch.rows == 1
        finally:
            await transactional.dispose()
        await asyncio.to_thread(migrate, "006", downgrade=True)
        async with engine.connect() as connection:
            await connection.run_sync(check_schema)
            assert (
                await connection.scalar(text("SELECT payloads_expired_at FROM async_operations WHERE id=1"))
            ) == 1000
            assert (await connection.scalar(text("SELECT result FROM async_operations WHERE id=2"))) == "active"
        await asyncio.to_thread(migrate, "head")
    finally:
        await engine.dispose()
        async with admin_engine.connect() as connection:
            await connection.execute(text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)"))
        await admin_engine.dispose()
