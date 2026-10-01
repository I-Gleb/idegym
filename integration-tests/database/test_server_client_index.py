"""Exercise concurrent index deployment and recovery on disposable databases."""

import io
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from idegym.orchestrator.migrations import server_client_index as index
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool

_ALEMBIC_INI = Path(index.__file__).parents[1] / "alembic.ini"


@pytest.fixture
def migration_database(db_url: str):
    name = f"idegym_test_index_{uuid4().hex}"
    admin = create_engine(make_url(db_url).set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    url = make_url(db_url).set(database=name)
    engine = create_engine(url.set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
    try:
        command.upgrade(config, "005")
        yield engine, config
    finally:
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def populate(connection):
    client_id = uuid4()
    connection.execute(text("INSERT INTO clients (id, name) VALUES (:id, 'index-test')"), {"id": client_id})
    connection.execute(
        text("INSERT INTO servers (client_id, generated_name, availability) VALUES (:id, 'existing', 'ALIVE')"),
        {"id": client_id},
    )
    return client_id


def version(connection):
    return connection.scalar(text("SELECT version_num FROM alembic_version"))


def run_cli(engine, *args):
    url = engine.url
    env = os.environ | {
        "POSTGRES_HOST": url.host or "localhost",
        "POSTGRES_PORT": str(url.port or 5432),
        "POSTGRES_USER": url.username or "postgres",
        "POSTGRES_PASSWORD": url.password or "",
        "POSTGRES_DB": url.database,
    }
    return subprocess.run(
        [sys.executable, "-m", index.__name__, *args], capture_output=True, text=True, env=env, timeout=20
    )


def test_empty_database_upgrade_creates_valid_index(migration_database):
    engine, config = migration_database
    command.upgrade(config, "006")
    with engine.connect() as connection:
        index.check_index(connection)
        assert version(connection) == "006"


def test_populated_database_requires_prebuild_and_preserves_old_startup_and_rollback(migration_database, tmp_path):
    engine, config = migration_database
    legacy = tmp_path / "migrations"
    shutil.copytree(_ALEMBIC_INI.parent / "migrations", legacy)
    for path in (legacy / "versions").iterdir():
        if path.is_file() and path.name[:3].isdigit() and int(path.name[:3]) > 5:
            path.unlink()
    old_config = Config(str(_ALEMBIC_INI))
    old_config.set_main_option("script_location", str(legacy))
    old_config.set_main_option("sqlalchemy.url", config.get_main_option("sqlalchemy.url"))
    with engine.connect() as connection:
        populate(connection)
    with pytest.raises(RuntimeError, match="Prebuild"):
        command.upgrade(config, "006")
    with engine.connect() as connection:
        assert version(connection) == "005"
        assert index.index_state(connection) is None

    for _ in range(2):
        result = run_cli(engine, "build")
        assert result.returncode == 0, result.stderr
        command.upgrade(old_config, "heads")
        with engine.connect() as connection:
            assert version(connection) == "005"
            index.check_index(connection)

    with engine.connect() as connection:
        index_oid = connection.scalar(text("SELECT 'public.ix_servers_client_id'::regclass::oid"))
    command.upgrade(config, "006")
    command.downgrade(config, "005")
    command.upgrade(old_config, "heads")
    with engine.connect() as connection:
        assert version(connection) == "005"
        assert connection.scalar(text("SELECT 'public.ix_servers_client_id'::regclass::oid")) == index_oid
        index.check_index(connection)
        assert connection.scalar(text("SELECT count(*) FROM servers")) == 1


def test_prebuild_respects_migration_lock(migration_database):
    engine, _ = migration_database
    with engine.connect() as connection:
        connection.execute(text("SELECT pg_advisory_lock(42239)"))
        result = run_cli(engine, "build")
        assert result.returncode != 0
        assert "migration lock 42239" in result.stderr
        assert index.index_state(connection) is None


@pytest.mark.parametrize("definition", ["availability", "client_id, availability"])
def test_prebuild_refuses_unexpected_same_name_index(migration_database, definition):
    engine, config = migration_database
    with engine.connect() as connection:
        connection.execute(text(f"CREATE INDEX ix_servers_client_id ON servers ({definition})"))
        oid = connection.scalar(text("SELECT 'public.ix_servers_client_id'::regclass::oid"))
        with pytest.raises(RuntimeError, match="unexpected definition"):
            index.ensure_index(connection, allow_populated_build=True, repair_invalid=True)
        assert connection.scalar(text("SELECT 'public.ix_servers_client_id'::regclass::oid")) == oid
    with pytest.raises(RuntimeError, match="unexpected definition"):
        command.upgrade(config, "006")


def test_interrupted_build_keeps_dml_available_and_recovers_invalid_index(migration_database):
    engine, config = migration_database
    with engine.connect() as monitor:
        client_id = populate(monitor)
        with engine.connect().execution_options(isolation_level="READ COMMITTED") as writer:
            writer.execute(text("UPDATE servers SET details = 'held transaction' WHERE generated_name = 'existing'"))
            with engine.connect() as builder, ThreadPoolExecutor(max_workers=1) as executor:
                pid = builder.scalar(text("SELECT pg_backend_pid()"))
                builder.execute(text("SET statement_timeout = '15s'"))
                future = executor.submit(index.ensure_index, builder, allow_populated_build=True)
                try:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        phase = monitor.scalar(
                            text("SELECT phase FROM pg_stat_progress_create_index WHERE pid = :pid"), {"pid": pid}
                        )
                        if phase == "waiting for writers before build":
                            break
                        if future.done():
                            future.result()
                            pytest.fail("The index build completed without waiting for the open writer")
                        time.sleep(0.01)
                    else:
                        pytest.fail("The index build did not reach its writer-wait phase")

                    monitor.execute(text("SET statement_timeout = '2s'"))
                    assert monitor.scalar(text("SELECT count(*) FROM servers")) == 1
                    monitor.execute(
                        text("INSERT INTO servers (client_id, generated_name) VALUES (:id, 'during-build')"),
                        {"id": client_id},
                    )
                    monitor.execute(text("UPDATE servers SET details='updated' WHERE generated_name='during-build'"))
                    assert monitor.scalar(text("SELECT pg_cancel_backend(:pid)"), {"pid": pid})
                    with pytest.raises(DBAPIError, match="canceling statement"):
                        future.result(timeout=5)
                finally:
                    monitor.execute(text("SELECT pg_cancel_backend(:pid)"), {"pid": pid})
                    writer.rollback()

        state = index.index_state(monitor)
        assert state is not None and not index.is_valid(state)
        assert version(monitor) == "005"
        with pytest.raises(RuntimeError, match="invalid"):
            index.ensure_index(monitor, allow_populated_build=True)

    with pytest.raises(RuntimeError, match="invalid"):
        command.upgrade(config, "006")
    result = run_cli(engine, "build", "--repair-invalid")
    assert result.returncode == 0, result.stderr
    command.upgrade(config, "006")
    with engine.connect() as connection:
        index.check_index(connection)
        oid = connection.scalar(text("SELECT 'public.ix_servers_client_id'::regclass::oid"))
        index.ensure_index(connection, allow_populated_build=True, repair_invalid=True)
        assert connection.scalar(text("SELECT 'public.ix_servers_client_id'::regclass::oid")) == oid
        assert connection.scalar(text("SELECT count(*) FROM servers")) == 2


def test_offline_upgrade_places_concurrent_build_outside_transaction():
    output = io.StringIO()
    config = Config(str(_ALEMBIC_INI), output_buffer=output)
    config.set_main_option("sqlalchemy.url", "postgresql+asyncpg://localhost/idegym_test_offline")
    command.upgrade(config, "005:006", sql=True)
    sql = output.getvalue()
    create = sql.index("CREATE INDEX CONCURRENTLY")
    assert sql.rfind("COMMIT;", 0, create) > sql.rfind("BEGIN;", 0, create)
    assert sql.index("BEGIN;", create) < sql.index("UPDATE alembic_version")


def test_interrupted_concurrent_drop_can_be_repaired(migration_database):
    engine, _ = migration_database
    with engine.connect() as monitor:
        client_id = populate(monitor)
        index.ensure_index(monitor, allow_populated_build=True)
        with engine.connect().execution_options(isolation_level="READ COMMITTED") as reader:
            reader.execute(text("SET LOCAL enable_seqscan = off"))
            reader.execute(text("SELECT id FROM servers WHERE client_id = :id"), {"id": client_id}).all()
            with engine.connect() as dropper, ThreadPoolExecutor(max_workers=1) as executor:
                pid = dropper.scalar(text("SELECT pg_backend_pid()"))
                dropper.execute(text("SET statement_timeout = '15s'"))
                future = executor.submit(dropper.execute, text("DROP INDEX CONCURRENTLY public.ix_servers_client_id"))
                try:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        state = index.index_state(monitor)
                        if state is not None and not index.is_valid(state):
                            break
                        if future.done():
                            future.result()
                            pytest.fail("The index drop completed without waiting for the open reader")
                        time.sleep(0.01)
                    else:
                        pytest.fail("The concurrent drop did not invalidate the index")
                    assert monitor.scalar(text("SELECT pg_cancel_backend(:pid)"), {"pid": pid})
                    with pytest.raises(DBAPIError, match="canceling statement"):
                        future.result(timeout=5)
                finally:
                    monitor.execute(text("SELECT pg_cancel_backend(:pid)"), {"pid": pid})
                    reader.rollback()
        assert not index.is_valid(index.index_state(monitor))
    result = run_cli(engine, "build", "--repair-invalid")
    assert result.returncode == 0, result.stderr
    with engine.connect() as connection:
        index.check_index(connection)
        assert version(connection) == "005"
        assert connection.scalar(text("SELECT count(*) FROM servers")) == 1
