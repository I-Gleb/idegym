"""Expire completed operations in bounded, caller-owned transactions."""

from typing import Literal

from idegym.orchestrator.database.models import TERMINAL_OPERATION_PREDICATE
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def clean_operation_batch(
    db: AsyncSession,
    *,
    action: Literal["payload", "metadata"],
    cutoff_ms: int,
    metadata_cutoff_ms: int,
    now_ms: int,
    batch_size: int,
) -> int:
    """Lock and mutate at most ``batch_size`` terminal rows, skipping concurrent cleaners.

    All timestamps are epoch milliseconds. The caller commits or rolls back the batch.
    Payload expiration clears both payloads and records their removal atomically.
    """
    if batch_size < 1 or action not in ("payload", "metadata"):
        raise ValueError("Retention requires a positive batch size and a payload or metadata action")
    predicate = TERMINAL_OPERATION_PREDICATE
    mutation = "DELETE FROM async_operations"
    if action == "payload":
        predicate += " AND payloads_expired_at IS NULL AND finished_at >= :metadata_cutoff"
        mutation = "UPDATE async_operations SET request = NULL, result = NULL, payloads_expired_at = :now"
    result = await db.execute(
        text(
            f"WITH candidates AS (SELECT id FROM async_operations WHERE {predicate} "
            "AND finished_at < :cutoff ORDER BY finished_at, id LIMIT :batch_size FOR UPDATE SKIP LOCKED) "
            f"{mutation} WHERE id IN (SELECT id FROM candidates)"
        ),
        {
            "cutoff": cutoff_ms,
            "metadata_cutoff": metadata_cutoff_ms,
            "now": now_ms,
            "batch_size": batch_size,
        },
    )
    return result.rowcount
