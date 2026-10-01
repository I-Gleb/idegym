# Operation retention

The watcher keeps completed-operation payloads for 24 hours and metadata for
14 days. Both ages start at `finished_at`. It preserves active operations and
rows without a completion time. Each transaction changes at most 500 rows,
uses the completion-time indexes, and skips rows locked by another cleaner.

Payload expiration clears `request` and `result` and sets
`payloads_expired_at` in the same transaction. The status endpoint then returns
HTTP 410 with `detail.code=operation_payload_expired` and operation metadata
in `detail.operation`. Clients must treat this as an unavailable result, not
replay a mutating operation. After metadata expires, the endpoint returns 404.

## Deploy

1. Apply [006_up.sql](src/idegym/orchestrator/migrations/versions/006_up.sql)
   with `psql -v ON_ERROR_STOP=1 -f 006_up.sql` against the target database,
   without `--single-transaction`. This adds the nullable column and builds
   both indexes concurrently. It leaves Alembic at revision 005, which the
   deployed reader accepts. A populated database refuses startup migration
   until both indexes are valid. If an index build fails, drop that invalid
   index concurrently and rerun the script.
2. Deploy the new orchestrator with payload expiration disabled. Startup
   advances Alembic to 006. Wait for every old reader to drain before enabling
   expiration; an old reader cannot distinguish an expired payload from an
   empty successful result.
3. Deploy the matching watcher. Set
   `IDEGYM_WATCHER_OPERATION_PAYLOAD_EXPIRATION_ENABLED=true` after the reader
   rollout finishes. Metadata cleanup runs even when payload expiration is
   disabled.

The watcher accepts these environment variables through its Hydra config:

| Variable | Default |
| --- | --- |
| `IDEGYM_WATCHER_OPERATION_PAYLOAD_EXPIRATION_ENABLED` | `false` |
| `IDEGYM_WATCHER_OPERATION_PAYLOAD_MAX_AGE` | `P1D` |
| `IDEGYM_WATCHER_REQUEST_MAX_AGE` | `P14D` |
| `IDEGYM_WATCHER_OPERATION_RETENTION_BATCH_SIZE` | `500` |
| `IDEGYM_WATCHER_OPERATION_RETENTION_TIMEOUT_MS` | `5000` |
| `IDEGYM_WATCHER_OPERATION_RETENTION_PAUSE` | `PT0.1S` |

Retention runs separately from pod cleanup. Full batches repeat after the
pause; an empty/partial pass or an error waits for the regular cleanup interval.
Deleted payload space becomes reusable after vacuum; database files and
PostgreSQL cache memory need not shrink.

## Roll back

Before any payload expires, stop the watcher and use the new image's Alembic
configuration to downgrade to 005 before starting an older orchestrator.
The downgrade retains the additive column and indexes and refuses to run if
any expiration marker exists. Configure Alembic's `sqlalchemy.url` for the
target database; the checked-in INI contains a placeholder.

After expiration starts, keep a reader that understands HTTP 410. Disable
payload expiration or scale the watcher to zero while fixing cleanup.
Application rollback cannot restore deleted payloads. Do not override the
downgrade guard or stamp 005 to make an old image start.
