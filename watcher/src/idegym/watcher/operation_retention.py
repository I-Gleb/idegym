"""Drain operation retention batches independently of pod cleanup."""

import asyncio

from idegym.api.config import WatcherConfig
from idegym.orchestrator.database.database import get_db_session
from idegym.orchestrator.database.models import current_time_millis
from idegym.orchestrator.database.operation_retention import clean_operation_batch
from idegym.utils.logging import get_logger
from sqlalchemy import text

logger = get_logger(__name__)


async def retain_operations_once(config: WatcherConfig) -> bool:
    """Commit one batch per action and return whether a batch filled its limit."""
    now = current_time_millis()
    metadata_cutoff = now - int(config.request_max_age.total_seconds() * 1000)
    cutoffs = {"metadata": metadata_cutoff}
    if config.operation_payload_expiration_enabled:
        cutoffs["payload"] = now - int(config.operation_payload_max_age.total_seconds() * 1000)
    full_batch = False
    for action, cutoff in cutoffs.items():
        async with asyncio.timeout(config.operation_retention_timeout_ms / 1000):
            async with get_db_session() as db, db.begin():
                await db.execute(
                    text(
                        "SELECT set_config('statement_timeout', :timeout, true), set_config('lock_timeout', '500ms', true)"
                    ),
                    {"timeout": str(config.operation_retention_timeout_ms)},
                )
                rows = await clean_operation_batch(
                    db,
                    action=action,
                    cutoff_ms=cutoff,
                    metadata_cutoff_ms=metadata_cutoff,
                    now_ms=now,
                    batch_size=config.operation_retention_batch_size,
                )
        full_batch |= rows == config.operation_retention_batch_size
        logger.debug("Operation retention batch committed", action=action, rows=rows)
    return full_batch


async def cleanup_operation_history(config: WatcherConfig) -> None:
    while True:
        delay = config.cleanup_interval.total_seconds()
        try:
            if await retain_operations_once(config):
                delay = config.operation_retention_pause.total_seconds()
        except Exception:
            logger.exception("Operation retention batch failed")
        await asyncio.sleep(delay)
