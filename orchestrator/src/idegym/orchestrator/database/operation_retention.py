"""Expire terminal-operation payloads and audit rows in caller-owned transactions."""

from dataclasses import dataclass
from typing import Literal

from idegym.orchestrator.database.models import (
    TERMINAL_OPERATION_PREDICATE,
    UNEXPIRED_OPERATION_PREDICATE,
    AsyncOperation,
)
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

RetentionAction = Literal["payload", "audit"]


@dataclass(frozen=True)
class RetentionBatch:
    rows: int = 0
    payload_bytes: int = 0


def eligible_operations(action: RetentionAction, cutoff_ms: int, minimum_finished_at_ms: int | None = None):
    # Literal status predicates also match the partial indexes in generic prepared plans.
    predicate = UNEXPIRED_OPERATION_PREDICATE if action == "payload" else TERMINAL_OPERATION_PREDICATE
    query = select(AsyncOperation.id).where(text(predicate), AsyncOperation.finished_at < cutoff_ms)
    if minimum_finished_at_ms is not None:
        query = query.where(AsyncOperation.finished_at >= minimum_finished_at_ms)
    return query


async def oldest_eligible_finished_at(
    db: AsyncSession, action: RetentionAction, cutoff_ms: int, minimum_finished_at_ms: int | None = None
) -> int | None:
    query = (
        eligible_operations(action, cutoff_ms, minimum_finished_at_ms)
        .with_only_columns(AsyncOperation.finished_at)
        .order_by(AsyncOperation.finished_at, AsyncOperation.id)
        .limit(1)
    )
    return (await db.execute(query)).scalar_one_or_none()


async def clean_operation_batch(
    db: AsyncSession,
    *,
    action: RetentionAction,
    cutoff_ms: int,
    expired_at_ms: int,
    batch_size: int,
    minimum_finished_at_ms: int | None = None,
) -> RetentionBatch:
    """Lock at most ``batch_size`` eligible rows and mutate both payload fields atomically.

    Times are epoch milliseconds. Rows with unknown completion times stay intact.
    The caller commits, rolls back on cancellation, and applies transaction timeouts.
    ``payload_bytes`` counts stored field bytes, excluding heap/index/TOAST overhead.
    """
    if action not in ("payload", "audit"):
        raise ValueError(f"Unknown retention action: {action}")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    stored_bytes = func.coalesce(func.pg_column_size(AsyncOperation.request), 0) + func.coalesce(
        func.pg_column_size(AsyncOperation.result), 0
    )
    candidates = (
        eligible_operations(action, cutoff_ms, minimum_finished_at_ms)
        .add_columns(stored_bytes.label("payload_bytes"))
        .order_by(AsyncOperation.finished_at, AsyncOperation.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
        .cte("candidates")
    )
    if action == "payload":
        mutation = update(AsyncOperation).values(request=None, result=None, payloads_expired_at=expired_at_ms)
    else:
        mutation = delete(AsyncOperation)
    query = (
        mutation.where(AsyncOperation.id == candidates.c.id)
        .returning(candidates.c.payload_bytes)
        .execution_options(synchronize_session=False)
    )
    sizes = (await db.execute(query)).scalars().all()
    return RetentionBatch(rows=len(sizes), payload_bytes=sum(sizes))
