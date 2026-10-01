"""Check retention age boundaries, transaction rollback and bounded progress."""

from contextlib import asynccontextmanager

from idegym.api.config import WatcherConfig
from idegym.orchestrator.database.models import AsyncOperation
from idegym.orchestrator.database.operation_retention import clean_operation_batch
from idegym.watcher import operation_retention
from sqlalchemy import select

DAY = 86400000
NOW = 30 * DAY


def operation(status="SUCCEEDED", finished_at=NOW - 2 * DAY):
    return AsyncOperation(
        request_type="FORWARD_REQUEST",
        status=status,
        request='{"request": "data"}',
        result='{"result": "data"}',
        scheduled_at=1,
        started_at=1,
        finished_at=finished_at,
    )


async def batch(db, action):
    return await clean_operation_batch(
        db,
        action=action,
        cutoff_ms=NOW - (DAY if action == "payload" else 14 * DAY),
        metadata_cutoff_ms=NOW - 14 * DAY,
        now_ms=NOW,
        batch_size=2,
    )


async def test_retention_batches_protect_active_and_recent_rows_and_preserve_metadata(db):
    eligible = [operation(status) for status in ("SUCCEEDED", "FAILED", "CANCELLED", "FINISHED_BY_WATCHER")]
    boundary = operation(finished_at=NOW - 14 * DAY)
    expired_metadata = operation(finished_at=NOW - 14 * DAY - 1)
    protected = [
        operation("IN_PROGRESS"),
        operation("SCHEDULED"),
        operation(finished_at=None),
        operation(finished_at=NOW - DAY),
        operation(finished_at=NOW - DAY + 1),
    ]
    db.add_all([*eligible, boundary, expired_metadata, *protected])
    await db.commit()
    expired_id = expired_metadata.id
    eligible_ids = {row.id for row in [*eligible, boundary]}
    protected_ids = {row.id for row in protected}

    assert await batch(db, "payload") == 2
    await db.rollback()
    db.expire_all()
    rows = (await db.execute(select(AsyncOperation))).scalars().all()
    assert all(row.payloads_expired_at is None and row.request and row.result for row in rows)

    assert await batch(db, "metadata") == 1
    await db.commit()
    counts = []
    while count := await batch(db, "payload"):
        assert count <= 2
        counts.append(count)
        await db.commit()
    assert counts == [2, 2, 1]
    await db.commit()
    db.expire_all()
    rows = {row.id: row for row in (await db.execute(select(AsyncOperation))).scalars()}
    assert expired_id not in rows
    for row_id in eligible_ids:
        row = rows[row_id]
        assert row.request is row.result is None
        assert row.payloads_expired_at == NOW
        assert row.finished_at is not None and row.request_type == "FORWARD_REQUEST"
    for row_id in protected_ids:
        row = rows[row_id]
        assert row.payloads_expired_at is None and row.request and row.result


async def test_payload_expiration_is_disabled_until_enabled(db, monkeypatch):
    row = operation()
    db.add(row)
    await db.commit()
    row_id = row.id

    @asynccontextmanager
    async def session():
        yield db

    monkeypatch.setattr(operation_retention, "get_db_session", session)
    monkeypatch.setattr(operation_retention, "current_time_millis", lambda: NOW)
    config = WatcherConfig()
    assert not await operation_retention.retain_operations_once(config)
    await db.refresh(row)
    assert row.request and row.result and row.payloads_expired_at is None
    await db.commit()

    config.operation_payload_expiration_enabled = True
    assert not await operation_retention.retain_operations_once(config)
    db.expire_all()
    row = (await db.execute(select(AsyncOperation).where(AsyncOperation.id == row_id))).scalar_one()
    assert row.request is row.result is None
    assert row.payloads_expired_at == NOW
