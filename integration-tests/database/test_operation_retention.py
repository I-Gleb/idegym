"""Verify retention transactions, completion races, and caller lifetimes on PostgreSQL."""

import asyncio
import json
import os
import sys
import textwrap
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import FastAPI, Response
from httpx import ASGITransport, AsyncClient
from idegym.api.config import OperationRetentionConfig, WatcherConfig
from idegym.api.orchestrator.operations import TERMINAL_ASYNC_OPERATION_STATUSES
from idegym.api.type import Duration
from idegym.orchestrator.database import database
from idegym.orchestrator.database.models import AsyncOperation, Client, IdeGYMServer
from idegym.orchestrator.database.operation_retention import clean_operation_batch
from idegym.orchestrator.router import async_operation, forwarding
from idegym.watcher import operation_retention as worker
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.datastructures import Headers

DAY_MS = 86400000
NOW = 1790856000000


def policy(**overrides):
    values = dict(payload_expiration_enabled=True, batch_size=2, max_rows_per_pass=20, max_pass_seconds=2)
    values.update(overrides)
    return WatcherConfig(operation_retention=OperationRetentionConfig(**values))


def operation(**values):
    fields = dict(
        request_type="FORWARD_REQUEST",
        status="SUCCEEDED",
        request='{"input":"request"}',
        result='{"body":"result"}',
        scheduled_at=NOW - 30 * DAY_MS,
        started_at=NOW - 30 * DAY_MS,
        finished_at=NOW - 2 * DAY_MS,
    )
    fields.update(values)
    return AsyncOperation(**fields)


@pytest.fixture
async def sessions(db, db_url):
    engine = create_async_engine(db_url, pool_size=5, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        await engine.dispose()


@pytest.mark.parametrize("action", ["payload", "audit"])
@pytest.mark.parametrize(
    "operation_status", [*sorted(TERMINAL_ASYNC_OPERATION_STATUSES), "IN_PROGRESS", "SCHEDULED", "UNKNOWN"]
)
async def test_only_terminal_rows_past_completion_grace_are_eligible(db, action, operation_status):
    cutoff = NOW - DAY_MS
    rows = [
        operation(status=operation_status, finished_at=finished, started_at=None)
        for finished in (None, cutoff - 1, cutoff, cutoff + 1, NOW + DAY_MS)
    ]
    db.add_all(rows)
    await db.commit()
    ids = [row.id for row in rows]
    batch = await clean_operation_batch(db, action=action, cutoff_ms=cutoff, expired_at_ms=NOW, batch_size=10)
    await db.commit()
    expected = int(operation_status in TERMINAL_ASYNC_OPERATION_STATUSES)
    assert batch.rows == expected
    for index, ident in enumerate(ids):
        row = await database.get_async_operation(db, ident)
        if expected and index == 1:
            if action == "audit":
                assert row is None
            else:
                assert row.request is row.result is None
                assert row.payloads_expired_at == NOW
                assert row.status == operation_status
                assert row.finished_at == cutoff - 1
        else:
            assert row.request is not None and row.result is not None
            assert row.payloads_expired_at is None


async def test_row_budget_is_shared_by_payload_and_audit_batches(db, sessions):
    db.add_all([operation() for _ in range(8)] + [operation(finished_at=NOW - 15 * DAY_MS) for _ in range(8)])
    await db.commit()
    result = await worker.run_retention_pass(policy(max_rows_per_pass=6), now_ms=NOW, session_provider=sessions)
    assert (result.audit_rows, result.payload_rows, result.reason) == (4, 2, "row_budget")
    assert result.payload_bytes > 0
    assert result.elapsed_seconds < 2


async def test_default_policy_retains_full_payloads_until_the_audit_cutoff(db, sessions):
    young = operation()
    old = operation(finished_at=NOW - 15 * DAY_MS)
    db.add_all([young, old])
    await db.commit()
    result = await worker.run_retention_pass(WatcherConfig(), now_ms=NOW, session_provider=sessions)
    assert result.payload_rows == 0 and result.audit_rows == 1
    assert (await database.get_async_operation(db, young.id)).result is not None
    assert await database.get_async_operation(db, old.id) is None


async def test_disabled_worker_does_not_checkout_a_connection():
    def unexpected_connection():
        raise AssertionError("disabled retention opened a session")

    result = await worker.run_retention_pass(policy(enabled=False), session_provider=unexpected_connection)
    assert result.rows == 0 and result.reason == "disabled"


async def test_old_active_and_unknown_completion_records_survive_retention(db, sessions):
    rows = [operation(status="IN_PROGRESS"), operation(status="SCHEDULED"), operation(finished_at=None)]
    db.add_all(rows)
    await db.commit()
    result = await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
    assert result.rows == 0
    assert (await db.scalar(select(func.count()).select_from(AsyncOperation))) == 3


async def test_old_scheduled_start_payload_still_participates_in_fifo_reuse(db, sessions):
    client = Client(id=uuid4(), name="retention-fifo")
    db.add(client)
    await db.commit()
    row = operation(
        client_id=client.id,
        request_type="START_SERVER",
        status="SCHEDULED",
        finished_at=None,
        request=json.dumps(
            {"image_tag": "sandbox:test", "runtime_class_name": "gvisor", "run_as_root": False, "server_kind": "idegym"}
        ),
    )
    db.add(row)
    await db.commit()
    assert (await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)).rows == 0
    assert await database.has_pending_start_server_operations(
        db,
        client_name=client.name,
        image_tag="sandbox:test",
        container_runtime="gvisor",
        run_as_root=False,
        server_kind="idegym",
        scheduled_before=NOW,
    )


async def test_existing_stale_marking_starts_a_new_retention_grace(db, sessions):
    rows = [operation(status="IN_PROGRESS", finished_at=None) for _ in range(3)]
    db.add_all(rows)
    await db.commit()
    assert await database.mark_stale_async_operations_as_finished(db, NOW, Duration(days=1)) == 3
    result = await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
    assert result.rows == 0
    status = await database.get_async_operation(db, rows[0].id)
    assert status.status == "FINISHED_BY_WATCHER" and status.finished_at == NOW and status.result is not None


async def test_concurrent_completion_is_skipped_then_gets_its_full_grace(db, sessions):
    completing, expiring = operation(), operation()
    db.add_all([completing, expiring])
    await db.commit()
    async with sessions() as writer:
        async with writer.begin():
            row = await writer.scalar(
                select(AsyncOperation).where(AsyncOperation.id == completing.id).with_for_update()
            )
            row.finished_at = NOW
            row.result = '"late completion"'
            await writer.flush()
            result = await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
            assert result.payload_rows == 1
    assert (await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)).rows == 0
    assert (await database.get_async_operation(db, completing.id)).result == '"late completion"'


async def test_late_writer_does_not_resurrect_expired_payloads_from_a_stale_identity_map(db, sessions):
    row = operation()
    db.add(row)
    await db.commit()
    await database.get_async_operation(db, row.id)
    await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
    updated = await database.update_async_operation(db, row.id, "SUCCEEDED", result={"late": "result"})
    assert updated.payloads_expired_at == NOW
    assert updated.request is updated.result is None
    assert updated.finished_at == NOW - 2 * DAY_MS


async def test_read_before_expiration_is_complete_and_later_poll_reports_expiration(db, sessions, mocker):
    row = operation(result='{"body":"' + "x" * 32768 + '"}')
    db.add(row)
    await db.commit()
    operation_id = row.id
    before = await db.scalar(select(AsyncOperation.result).where(AsyncOperation.id == operation_id))
    await db.rollback()
    await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
    assert json.loads(before)["body"] == "x" * 32768
    mocker.patch.object(database, "SessionFactory", sessions)
    app = FastAPI()
    app.include_router(async_operation.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/api/operations/status/{operation_id}")
    assert response.status_code == 410
    assert response.json()["detail"]["operation"]["payloads_expired_at"] == NOW


async def test_cancellation_rolls_back_only_the_current_batch_and_restart_resumes(db, sessions, monkeypatch):
    db.add_all([operation() for _ in range(3)])
    await db.commit()
    uncommitted = asyncio.Event()
    original = worker.clean_operation_batch
    payload_batches = 0

    async def pause_second_batch(*args, **kwargs):
        nonlocal payload_batches
        result = await original(*args, **kwargs)
        if kwargs["action"] == "payload":
            payload_batches += 1
            if payload_batches == 2:
                uncommitted.set()
                await asyncio.Event().wait()
        return result

    monkeypatch.setattr(worker, "clean_operation_batch", pause_second_batch)
    task = asyncio.create_task(worker.run_retention_pass(policy(batch_size=1), now_ms=NOW, session_provider=sessions))
    await asyncio.wait_for(uncommitted.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with sessions() as reader:
        count = await reader.scalar(select(func.count()).where(AsyncOperation.payloads_expired_at.is_not(None)))
        assert count == 1
    monkeypatch.setattr(worker, "clean_operation_batch", original)
    resumed = await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
    assert resumed.payload_rows == 2
    assert (await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)).rows == 0


async def test_worker_process_crash_preserves_committed_batches_and_restart_resumes(db, db_url, sessions):
    db.add_all([operation() for _ in range(3)])
    await db.commit()
    program = textwrap.dedent("""
        import asyncio, logging, os, structlog
        from idegym.api.config import OperationRetentionConfig, WatcherConfig
        from idegym.watcher import operation_retention as worker
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))
        original = worker.clean_operation_batch
        payload_batches = 0
        async def pause_second_batch(*args, **kwargs):
            global payload_batches
            result = await original(*args, **kwargs)
            if kwargs['action'] == 'payload':
                payload_batches += 1
                if payload_batches == 2:
                    print('batch-uncommitted', flush=True)
                    await asyncio.Event().wait()
            return result

        async def main():
            engine = create_async_engine(os.environ['RETENTION_TEST_DATABASE_URL'])
            sessions = async_sessionmaker(engine)
            worker.clean_operation_batch = pause_second_batch
            config = WatcherConfig(operation_retention=OperationRetentionConfig(
                payload_expiration_enabled=True, batch_size=1, max_rows_per_pass=10,
                max_pass_seconds=60, batch_timeout_seconds=10))
            await worker.run_retention_pass(config, now_ms=1790856000000, session_provider=sessions)

        asyncio.run(main())
    """)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        program,
        env=dict(os.environ, RETENTION_TEST_DATABASE_URL=db_url),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"batch-uncommitted\n"
        process.kill()
        await process.wait()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    async with sessions() as reader:
        assert await reader.scalar(select(func.count()).where(AsyncOperation.payloads_expired_at.is_not(None))) == 1
    resumed_rows = 0
    async with asyncio.timeout(2):
        while resumed_rows < 2:
            result = await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)
            resumed_rows += result.payload_rows
            await asyncio.sleep(0.01)
    assert resumed_rows == 2


async def test_multiple_cleanup_contenders_commit_each_row_once(db, sessions):
    db.add_all([operation() for _ in range(40)])
    await db.commit()
    passes = await asyncio.gather(
        *(
            worker.run_retention_pass(policy(max_rows_per_pass=100), now_ms=NOW, session_provider=sessions)
            for _ in range(4)
        )
    )
    assert sum(result.payload_rows for result in passes) == 40
    assert all(result.reason == "empty" for result in passes)


async def test_pool_checkout_is_included_in_the_batch_deadline(db, db_url):
    engine = create_async_engine(db_url, pool_size=1, max_overflow=0, pool_timeout=10)
    factory = async_sessionmaker(engine)
    try:
        async with engine.connect():
            result = await worker.run_retention_pass(
                policy(max_pass_seconds=0.1, batch_timeout_seconds=0.05), now_ms=NOW, session_provider=factory
            )
        assert result.reason == "timeout"
        assert result.rows == 0 and result.elapsed_seconds < 0.5
        assert (await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=factory)).reason == "empty"
    finally:
        await engine.dispose()


async def test_statement_deadline_rolls_back_and_releases_the_connection(db, sessions):
    db.add(operation())
    await db.commit()
    await db.execute(
        text(
            "CREATE FUNCTION retention_test_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$"
        )
    )
    await db.execute(
        text(
            "CREATE TRIGGER retention_test_delay BEFORE UPDATE ON async_operations FOR EACH ROW EXECUTE FUNCTION retention_test_delay()"
        )
    )
    await db.commit()
    try:
        result = await worker.run_retention_pass(
            policy(max_pass_seconds=0.2, batch_timeout_seconds=0.1), now_ms=NOW, session_provider=sessions
        )
        assert result.reason in {"timeout", "error"}
        assert result.rows == 0 and result.elapsed_seconds < 0.5
        assert await db.scalar(select(AsyncOperation.payloads_expired_at)) is None
    finally:
        await db.execute(text("DROP TRIGGER retention_test_delay ON async_operations"))
        await db.execute(text("DROP FUNCTION retention_test_delay()"))
        await db.commit()
    assert (await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)).payload_rows == 1


async def test_backend_death_rolls_back_an_uncommitted_batch(db, sessions):
    row = operation()
    db.add(row)
    await db.commit()
    async with sessions() as victim:
        pid = await victim.scalar(text("SELECT pg_backend_pid()"))
        await clean_operation_batch(victim, action="payload", cutoff_ms=NOW - DAY_MS, expired_at_ms=NOW, batch_size=1)
        await db.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": pid})
        await db.commit()
        try:
            await victim.rollback()
        except Exception:
            await victim.invalidate()
    assert (await database.get_async_operation(db, row.id)).result is not None
    assert (await worker.run_retention_pass(policy(), now_ms=NOW, session_provider=sessions)).payload_rows == 1


@pytest.mark.parametrize("wait_seconds", [0, 1])
@pytest.mark.parametrize("status_code", [200, 422])
async def test_direct_and_polled_forward_results_survive_the_completion_grace(
    db, sessions, mocker, wait_seconds, status_code
):
    client = Client(id=uuid4(), name="retention-test")
    db.add(client)
    await db.flush()
    server = IdeGYMServer(
        client_id=client.id, client_name=client.name, generated_name="retention-test", server_name="test"
    )
    db.add(server)
    await db.commit()
    mocker.patch.object(database, "SessionFactory", sessions)
    mocker.patch.object(database, "current_time_millis", return_value=NOW)
    mocker.patch.object(
        forwarding, "validate_server", return_value=SimpleNamespace(pod_ip="10.0.0.1", container_port=8000)
    )
    mocker.patch.object(forwarding, "forward_request_internally", return_value=(status_code, {}, "tool result"))
    mocker.patch.object(forwarding, "update_server_heartbeat_on_call")
    response = Response(status_code=202)
    result = await forwarding.forward_request_to_server(
        client_id=client.id,
        server_id=server.id,
        path="api/tools/bash",
        method="POST",
        headers=Headers({}),
        body='{"command":"true"}',
        http_client=None,
        wait_seconds=wait_seconds,
        response=response,
    )
    async with asyncio.timeout(2):
        while True:
            async with sessions() as reader:
                op = await reader.scalar(select(AsyncOperation))
            if op is not None and op.status in TERMINAL_ASYNC_OPERATION_STATUSES:
                break
            await asyncio.sleep(0.01)
    if wait_seconds:
        assert response.status_code == 200 and result.body == "tool result"
    else:
        assert response.status_code == 202 and result.async_operation_id == op.id
    assert (await worker.run_retention_pass(policy(), now_ms=NOW + DAY_MS, session_provider=sessions)).rows == 0
    status = await async_operation.get_operation_status(op.id)
    assert json.loads(status.result)["body"] == "tool result"
    assert status.status == ("SUCCEEDED" if status_code == 200 else "FAILED")
