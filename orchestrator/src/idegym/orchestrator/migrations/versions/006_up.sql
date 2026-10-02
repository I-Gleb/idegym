SET lock_timeout = '2s';
SET statement_timeout = '30min';
SET max_parallel_maintenance_workers = 0;

ALTER TABLE public.async_operations ADD COLUMN IF NOT EXISTS payloads_expired_at BIGINT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_async_operations_terminal_finished
ON public.async_operations (finished_at, id)
WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'FINISHED_BY_WATCHER') AND finished_at IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_async_operations_payload_retention
ON public.async_operations (finished_at, id)
WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'FINISHED_BY_WATCHER')
AND finished_at IS NOT NULL AND payloads_expired_at IS NULL;
