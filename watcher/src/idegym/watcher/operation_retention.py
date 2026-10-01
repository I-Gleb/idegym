"""Run bounded operation retention independently of pod lifecycle cleanup."""

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

from idegym.api.config import WatcherConfig
from idegym.orchestrator.database.database import get_db_session
from idegym.orchestrator.database.models import current_time_millis
from idegym.orchestrator.database.operation_retention import (
    RetentionAction,
    RetentionBatch,
    clean_operation_batch,
    oldest_eligible_finished_at,
)
from idegym.utils.logging import get_logger
from prometheus_client import Counter, Gauge, Histogram
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
SessionProvider = Callable[[], AbstractAsyncContextManager[AsyncSession]]

ROWS = Counter("idegym_operation_retention_rows", "Rows in confirmed committed retention batches", ["action"])
PAYLOAD_BYTES = Counter(
    "idegym_operation_retention_payload_bytes", "Stored field bytes removed in confirmed commits", ["action"]
)
BATCHES = Counter("idegym_operation_retention_batches", "Retention batch outcomes", ["action", "outcome"])
BATCH_SECONDS = Histogram("idegym_operation_retention_batch_seconds", "Retention transaction time", ["action"])
PASSES = Counter("idegym_operation_retention_passes", "Retention pass stop reasons", ["reason"])
PASS_SECONDS = Histogram("idegym_operation_retention_pass_seconds", "Retention pass time")
OLDEST_LAG = Gauge(
    "idegym_operation_retention_oldest_eligible_lag_seconds",
    "Age beyond the retention cutoff of the oldest eligible row at the last batch start",
    ["action"],
)


@dataclass
class RetentionPass:
    payload_rows: int = 0
    audit_rows: int = 0
    payload_bytes: int = 0
    reason: str = "empty"
    elapsed_seconds: float = 0.0

    @property
    def rows(self) -> int:
        return self.payload_rows + self.audit_rows


async def _run_batch(
    session_provider: SessionProvider,
    *,
    action: RetentionAction,
    cutoff_ms: int,
    minimum_finished_at_ms: int | None,
    now_ms: int,
    batch_size: int,
    timeout_seconds: float,
) -> RetentionBatch:
    started = asyncio.get_running_loop().time()
    try:
        async with asyncio.timeout(timeout_seconds):
            async with session_provider() as db:
                async with db.begin():
                    timeout_ms = max(1, int(timeout_seconds * 900))
                    await db.execute(
                        text(
                            "SELECT set_config('statement_timeout', :statement, true), "
                            "set_config('lock_timeout', :lock, true), "
                            "set_config('idle_in_transaction_session_timeout', :idle, true)"
                        ),
                        {
                            "statement": f"{timeout_ms}ms",
                            "lock": f"{min(100, timeout_ms)}ms",
                            "idle": f"{max(1, int(timeout_seconds * 1000))}ms",
                        },
                    )
                    oldest = await oldest_eligible_finished_at(db, action, cutoff_ms, minimum_finished_at_ms)
                    OLDEST_LAG.labels(action).set(0 if oldest is None else max(0, cutoff_ms - oldest) / 1000)
                    batch = await clean_operation_batch(
                        db,
                        action=action,
                        cutoff_ms=cutoff_ms,
                        expired_at_ms=now_ms,
                        batch_size=batch_size,
                        minimum_finished_at_ms=minimum_finished_at_ms,
                    )
        ROWS.labels(action).inc(batch.rows)
        PAYLOAD_BYTES.labels(action).inc(batch.payload_bytes)
        BATCHES.labels(action, "committed").inc()
        return batch
    except asyncio.CancelledError:
        BATCHES.labels(action, "cancelled").inc()
        raise
    except TimeoutError:
        BATCHES.labels(action, "timeout").inc()
        raise
    except Exception:
        BATCHES.labels(action, "error").inc()
        raise
    finally:
        BATCH_SECONDS.labels(action).observe(asyncio.get_running_loop().time() - started)


async def run_retention_pass(
    watcher_config: WatcherConfig, *, now_ms: int | None = None, session_provider: SessionProvider = get_db_session
) -> RetentionPass:
    """Commit small batches until the row/time budget is spent or eligible work is exhausted.

    Each batch owns its connection and transaction. Concurrent workers skip locked rows;
    cancellation rolls back the current transaction and leaves committed batches intact.
    """
    config = watcher_config.operation_retention
    summary = RetentionPass()
    loop = asyncio.get_running_loop()
    started = loop.time()
    deadline = started + config.max_pass_seconds
    now_ms = current_time_millis() if now_ms is None else now_ms
    audit_cutoff = now_ms - int(watcher_config.request_max_age.total_seconds() * 1000)
    cutoffs: dict[RetentionAction, int] = {"audit": audit_cutoff}
    if config.payload_expiration_enabled:
        cutoffs["payload"] = now_ms - int(config.payload_max_age.total_seconds() * 1000)
    else:
        OLDEST_LAG.labels("payload").set(0)
    exhausted: set[RetentionAction] = set()
    try:
        if not config.enabled:
            summary.reason = "disabled"
            return summary
        while len(exhausted) < len(cutoffs):
            for action, cutoff_ms in cutoffs.items():
                if action in exhausted:
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    summary.reason = "time_budget"
                    return summary
                if summary.rows >= config.max_rows_per_pass:
                    summary.reason = "row_budget"
                    return summary
                limit = min(config.batch_size, config.max_rows_per_pass - summary.rows)
                batch = await _run_batch(
                    session_provider,
                    action=action,
                    cutoff_ms=cutoff_ms,
                    # Rows past the audit cutoff are deleted directly, without a preceding payload rewrite.
                    minimum_finished_at_ms=audit_cutoff if action == "payload" else None,
                    now_ms=now_ms,
                    batch_size=limit,
                    timeout_seconds=min(config.batch_timeout_seconds, remaining),
                )
                if action == "audit":
                    summary.audit_rows += batch.rows
                else:
                    summary.payload_rows += batch.rows
                summary.payload_bytes += batch.payload_bytes
                if batch.rows < limit:
                    exhausted.add(action)
                await asyncio.sleep(0)
        return summary
    except asyncio.CancelledError:
        summary.reason = "cancelled"
        raise
    except TimeoutError:
        summary.reason = "timeout"
        return summary
    except Exception as exc:
        summary.reason = "error"
        logger.warning("Operation retention batch failed", error_type=type(exc).__name__)
        return summary
    finally:
        summary.elapsed_seconds = loop.time() - started
        PASSES.labels(summary.reason).inc()
        PASS_SECONDS.observe(summary.elapsed_seconds)
        logger.info(
            "Operation retention pass finished",
            reason=summary.reason,
            payload_rows=summary.payload_rows,
            audit_rows=summary.audit_rows,
            payload_bytes=summary.payload_bytes,
            elapsed_seconds=summary.elapsed_seconds,
        )


async def cleanup_expired_operations(watcher_config: WatcherConfig):
    while True:
        await asyncio.sleep(watcher_config.operation_retention.interval.total_seconds())
        await run_retention_pass(watcher_config)
