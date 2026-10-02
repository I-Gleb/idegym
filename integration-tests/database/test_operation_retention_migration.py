"""Check retention preparation, upgrade and rollback on PostgreSQL."""

from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

ORCHESTRATOR = Path(__file__).resolve().parents[2] / "orchestrator/src/idegym/orchestrator"


@pytest.fixture
def migration_db(db_url):
    url = make_url(db_url).set(drivername="postgresql+psycopg2")
    name = "retention_" + uuid4().hex
    admin = create_engine(url, isolation_level="AUTOCOMMIT")
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{name}"'))
    engine = create_engine(url.set(database=name), isolation_level="AUTOCOMMIT")
    config = Config(str(ORCHESTRATOR / "alembic.ini"))
    config.set_main_option(
        "sqlalchemy.url",
        url.set(drivername="postgresql+asyncpg", database=name)
        .render_as_string(hide_password=False)
        .replace("%", "%%"),
    )
    try:
        with engine.connect() as connection:
            yield config, connection
    finally:
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def test_empty_database_upgrades_without_preparation(migration_db):
    config, connection = migration_db
    command.upgrade(config, "head")
    assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "006"


def test_populated_database_requires_preparation_and_guards_rollback(migration_db):
    config, connection = migration_db
    command.upgrade(config, "005")
    connection.execute(
        text(
            "INSERT INTO async_operations (request_type, status, request, result, scheduled_at, finished_at) VALUES ('FORWARD_REQUEST', 'SUCCEEDED', 'request', 'result', 1, 2)"
        )
    )
    with pytest.raises(RuntimeError, match="Apply 006_up.sql"):
        command.upgrade(config, "head")

    for statement in (ORCHESTRATOR / "migrations/versions/006_up.sql").read_text().split(";"):
        if statement.strip():
            connection.execute(text(statement))
    assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "005"
    command.upgrade(config, "head")
    command.downgrade(config, "005")
    assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "005"
    assert connection.execute(text("SELECT request, result FROM async_operations")).one() == ("request", "result")

    command.upgrade(config, "head")
    connection.execute(text("UPDATE async_operations SET request = NULL, result = NULL, payloads_expired_at = 3"))
    with pytest.raises(RuntimeError, match="compatible reader"):
        command.downgrade(config, "005")
    assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "006"
