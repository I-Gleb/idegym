"""Verify live lookups, history access, and quota populations against PostgreSQL."""

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from idegym.api.orchestrator.clients import AvailabilityStatus
from idegym.api.orchestrator.servers import AliveServerInfo
from idegym.orchestrator.database import helpers
from idegym.orchestrator.database.database import (
    QUOTA_HOLDING_STATUSES,
    create_client,
    get_alive_idegym_servers_by_client_id,
    get_idegym_servers_by_client_id,
    get_idegym_servers_by_status,
    update_idegym_server_heartbeat,
    update_idegym_server_owner,
)
from idegym.orchestrator.database.models import IdeGYMServer
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession


async def test_live_lookup_projects_only_live_identity_and_preserves_history_and_quota(db: AsyncSession, monkeypatch):
    clients = [await create_client(db, "same-name"), await create_client(db, "same-name")]
    servers = [
        IdeGYMServer(
            client_id=client.id,
            client_name=client.name,
            generated_name=f"client-{index}-{status}",
            availability=status,
            pod_manifest={"payload": "manifest" * 50_000},
            details="details" * 50_000,
        )
        for index, client in enumerate(clients)
        for status in AvailabilityStatus
    ]
    db.add_all(servers)
    await db.commit()
    expected = {
        client.id: [
            AliveServerInfo(id=server.id, generated_name=server.generated_name)
            for server in servers
            if server.client_id == client.id
            and server.availability in {AvailabilityStatus.ALIVE, AvailabilityStatus.REUSED}
        ]
        for client in clients
    }
    db.expunge_all()
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    @asynccontextmanager
    async def session():
        yield db

    monkeypatch.setattr(helpers, "get_db_session", session)
    engine = db.bind.sync_engine
    event.listen(engine, "before_cursor_execute", capture)
    try:
        for client in clients:
            statements.clear()
            result = await helpers.find_alive_servers(client_id=client.id)
            assert sorted(result, key=lambda row: row.id) == sorted(expected[client.id], key=lambda row: row.id)
            assert len(statements) == 1
            projection = statements[0].split("FROM", 1)[0]
            assert "servers.id" in projection and "servers.generated_name" in projection
            assert all(
                column not in projection for column in ("details", "pod_manifest", "availability", "client_name")
            )
            assert "availability IN" in statements[0]
            assert "'ALIVE'" in statements[0] and "'REUSED'" in statements[0]
            assert "client_id =" in statements[0]
            assert not db.identity_map
    finally:
        event.remove(engine, "before_cursor_execute", capture)

    for client in clients:
        history = await get_idegym_servers_by_client_id(db, client.id)
        assert {server.availability for server in history} == set(AvailabilityStatus)
        assert {server.client_id for server in history} == {client.id}
        assert all(server.pod_manifest["payload"] == "manifest" * 50_000 for server in history)
        assert all(server.details == "details" * 50_000 for server in history)

    accounted = await get_idegym_servers_by_status(db, QUOTA_HOLDING_STATUSES)
    assert len(accounted) == 6
    assert {server.availability for server in accounted} == {
        AvailabilityStatus.ALIVE,
        AvailabilityStatus.REUSED,
        AvailabilityStatus.FINISHED,
    }


@pytest.mark.parametrize("population", ["unknown", "empty", "terminal"])
async def test_live_lookup_returns_empty(db: AsyncSession, population: str):
    client_id = uuid4()
    if population != "unknown":
        client = await create_client(db, "empty")
        client_id = client.id
        if population == "terminal":
            db.add(IdeGYMServer(client_id=client.id, generated_name="killed", availability=AvailabilityStatus.KILLED))
            await db.commit()
    assert await get_alive_idegym_servers_by_client_id(db, client_id) == []


async def test_live_lookup_follows_reused_server_owner(db: AsyncSession):
    previous = await create_client(db, "previous")
    current = await create_client(db, "current")
    server = IdeGYMServer(client_id=previous.id, generated_name="reusable", availability=AvailabilityStatus.FINISHED)
    db.add(server)
    await db.commit()

    assert await get_alive_idegym_servers_by_client_id(db, previous.id) == []
    await update_idegym_server_owner(db, server.id, current.id)
    assert await get_alive_idegym_servers_by_client_id(db, current.id) == []
    await update_idegym_server_heartbeat(db, server.id, AvailabilityStatus.REUSED)
    assert await get_alive_idegym_servers_by_client_id(db, previous.id) == []
    assert await get_alive_idegym_servers_by_client_id(db, current.id) == [
        AliveServerInfo(id=server.id, generated_name="reusable")
    ]
    assert await get_idegym_servers_by_client_id(db, previous.id) == []
