# Index live server lookups

Client stop and finish requests now read only `id` and `generated_name` for
their client's `ALIVE` and `REUSED` servers. Revision `006` adds a concurrent
B-tree index on `servers(client_id)`. The implementation is verified locally;
image builds and the coordinated production rollout remain pending.

The source base is `mellum-main@ef6a8bff12e537afb03ee4fa8ebbacacd9ed607f`.
The fetched upstream branch and all seven bundled deployed-source references
match this base. [Source provenance](live-server-queries-evidence/source-provenance.json)
records the hashes. The change preserves plain-Pod creation, pod-IP forwarding,
blocking forwards, and quota reconciliation from this base.

## Evidence and behavior

The historical lookup loads full server history and filters live status in
Python. With no client index, its captured plan scans the entire `servers`
table in parallel. The [historical plan](live-server-queries-evidence/historical-server-plan.txt)
projects manifests and failure details as well as server identities. Changing
the planner cache estimate from 6 to 24 GiB leaves that plan unchanged in the
[captured comparison](live-server-queries-evidence/historical-cache-plans.txt).
The incident's cumulative scan counters are not isolated query timings.

The [production read at 11:05 UTC on October 1, 2026](live-server-queries-evidence/production-read-20261001.json)
finds PostgreSQL 16.15, Alembic `005`, approximately 571,961 server rows, and no
client index. A read-only `EXPLAIN` of the projected live query uses the
existing status index. It uses the zero UUID and does not execute the query.
The earlier deployment read finds 48 available orchestrators and one watcher,
all on `reconcile-ef6a8bf`. These observations require refresh before rollout.

`get_idegym_servers_by_client_id` retains its full-history behavior.
`find_alive_servers`, used by `DELETE /api/clients` and
`POST /api/clients/finish`, calls the new projected query. Its result shape
and unspecified ordering remain the same. The client UUID stays parameterized;
the two fixed statuses become SQL literals so PostgreSQL can use the existing
partial index in a generic prepared plan. Quota queries continue to include
`FINISHED` as well as `ALIVE` and `REUSED`.

The [local benchmark](live-server-queries-evidence/benchmark.json) uses PostgreSQL
16.2 on macOS ARM64, 600,000 rows, 1,000 clients, 9,000 live servers, and 1,000
`FINISHED` servers. The table occupies 307,281,920 bytes. Each row contains two
compressible payloads of about 4 KiB. The history-heavy client owns 124,500 rows,
including 4,500 live rows. The typical client owns 477 rows, including five live
rows. Timings are medians of three `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` runs;
cache state is not reset between runs.

| Lookup | Returned rows | Heap rows examined | Shared buffers | Median execution |
| --- | ---: | ---: | ---: | ---: |
| Typical client, old history query, before index | 477 | 600,000 | 37,497 | 42.109 ms |
| Typical client, new live query, after index | 5 | 5 | 18 | 0.168 ms |
| Typical client, new generic prepared plan | 5 | 5 | 18 | 0.177 ms |
| History-heavy client, old query, before index | 124,500 | 600,000 | 37,497 | 78.412 ms |
| History-heavy client, new generic prepared plan | 4,500 | 4,500 | 402 | 2.807 ms |
| Missing client, old query, before index | 0 | 600,000 | 37,497 | 43.885 ms |
| Missing client, new generic prepared plan | 0 | 0 | 3 | 0.006 ms |

The new client index occupies 4,366,336 bytes in this fixture. PostgreSQL
combines it with `ix_servers_live` for selective live lookups. A custom plan for
the history-heavy client uses the status index alone: 9,000 rows examined,
1,138 buffers, 1.144 ms. No additional composite or partial index is included;
the measured plans support retaining these two indexes. Reassess with the
production client distribution and latency budget during rollout.

The comparison also captures generic plans with bound status values. For the
history-heavy client, that form examines 124,500 heap rows after indexing and
takes 30.189 ms; keeping the fixed statuses literal reduces this to 4,500 rows
and 2.807 ms. The client index also serves history lookups and missing clients.

With the index present, an actual fetch of the typical client's history
materializes 477 rows and 3,938,589 payload characters. The new query returns
five identities and no manifest/details payloads, with the same live result
set. Those single fetches take 12.086 and 0.872 ms respectively. These are local
measurements, not a production latency or capacity forecast.

## Verify locally

From the repository root:

```bash
uv sync --frozen --all-packages --all-groups --python 3.12
uv run --no-sync pytest integration-tests/database \
  unit-tests/test_migrations.py unit-tests/test_reconcile.py \
  unit-tests/test_orchestrator_forwarding.py unit-tests/test_kubernetes_client_pods.py -q
```

The database fixture uses PostgreSQL 16 in Docker by default. An external
disposable instance can be supplied through `IDEGYM_TEST_DATABASE_URL`, with
driver `postgresql+asyncpg` and database name beginning `idegym_test_`. Its user
needs `CREATEDB` for isolated migration databases. The tests create and truncate
tables; the URL must never identify a service database.

The recorded run uses Python 3.12.14 and `pgserver==0.1.4`, which supplies
PostgreSQL 16.2. Its setup is:

```bash
uv pip install pgserver==0.1.4
TASK03_PG_BIN=.venv/lib/python3.12/site-packages/pgserver/pginstall/bin
"$TASK03_PG_BIN/initdb" -D /private/tmp/rl-9000-task03-pgdata -U idegym_test --auth=trust -E UTF8 --no-locale
mkdir -p /private/tmp/rl-9000-task03-pgsocket
"$TASK03_PG_BIN/pg_ctl" -D /private/tmp/rl-9000-task03-pgdata \
  -l /private/tmp/rl-9000-task03-postgres.log \
  -o '-h 127.0.0.1 -p 55439 -k /private/tmp/rl-9000-task03-pgsocket' -w start
"$TASK03_PG_BIN/createdb" -h 127.0.0.1 -p 55439 -U idegym_test idegym_test_task03
export IDEGYM_TEST_DATABASE_URL=postgresql+asyncpg://idegym_test@127.0.0.1:55439/idegym_test_task03
```

Run the test command above with that environment. To reproduce the populated
comparison, run:

```bash
uv run --no-sync python scripts/benchmark_live_server_queries.py \
  --output /private/tmp/live-server-query-plans.json --rows 600000 --repeats 3
```

The benchmark requires localhost and creates and removes its own database.
After verification, stop the disposable server:

```bash
"$TASK03_PG_BIN/pg_ctl" -D /private/tmp/rl-9000-task03-pgdata -m fast -w stop
```

The targeted suite passes 162 tests. It covers multiple clients, every server status, empty and
unknown clients, ownership transfer, full-history payload access, quota counts,
and the actual helper's SQL projection. Migration checks exercise an empty
database, a populated database's startup guard, prebuild replay, the advisory
lock, unexpected index definitions, cancellation of concurrent creation and
drop, invalid-index repair, and application-version rollback. Reads, inserts,
and updates complete while an index build waits for an older writer.

Ruff and `git diff --check` pass. The orchestrator wheel includes the helper,
revision `006`, and both SQL files. The PostgreSQL minor version and architecture
differ from production. Docker builds, Kubernetes admission/readiness, production
I/O under index construction, and 8,500–9,000 simultaneous sandbox behavior
remain unverified. Existing Alembic `path_separator` deprecation warnings do not
fail the tests.

## Prepare the online rollout

1. Refresh images, Alembic heads, index definitions/validity, table estimates,
   free database/WAL/temp space, active transactions, and serving replica counts.
   Use a bounded read-only transaction for catalog queries and plain `EXPLAIN`.
   Production `EXPLAIN ANALYZE` requires a separate workload plan.
2. Integrate `006` before task 05's `007`, preserving one Alembic head. Build the
   orchestrator from the reviewed candidate with the frozen lockfile and pin its
   image digest. Review the pool and rollout budget with task 04 before replacing
   any replicas. The prebuild command adds one database connection.
3. Run the candidate image's prebuild command in a separate migration Job while
   all existing orchestrators keep serving:

   ```bash
   /opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index build \
     --lock-timeout-seconds 5 --statement-timeout-seconds 1800
   ```

   It reads `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
   when required, and `POSTGRES_DB`. The supplied GKE manifests use
   `idegym-postgres:5432`, user/database `idegym`, namespace `mellum`.
   It holds session advisory lock `42239`, runs DDL in autocommit, validates the
   index, and leaves `alembic_version` at `005`.
4. Use the [Job template](prebuild-server-client-index.job.yaml) after replacing
   its image placeholder with the candidate digest and reviewing placement,
   admission, registry access, and resources against current configuration.
   Its 100m CPU/128Mi request and 1 CPU/256Mi limit are provisional allowances
   for the single client process. Index construction consumes PostgreSQL's
   resources. Its five-second lock timeout, 30-minute statement timeout, and
   40-minute Job deadline also require review. `backoffLimit: 0` leaves failures
   visible for inspection. The Job has a distinct `app` label so it cannot join
   the serving orchestrator Service.

   After the rollout owner approves the rendered file, create the Job and save
   the returned name:

   ```bash
   TASK03_INDEX_JOB=$(kubectl --context gke-europe-west1 --namespace mellum \
     create -f /private/tmp/prebuild-server-client-index.reviewed.yaml -o jsonpath='{.metadata.name}')
   kubectl --context gke-europe-west1 --namespace mellum \
     wait --for=condition=complete "job/$TASK03_INDEX_JOB" --timeout=40m
   kubectl --context gke-europe-west1 --namespace mellum logs "job/$TASK03_INDEX_JOB"
   ```

5. Run the candidate's `server_client_index check` command. Require the expected
   B-tree definition and all of `indisvalid`, `indisready`, `indislive` to be true.
   Confirm Alembic is still `005`. Repeating `build` retains a valid index.
6. Roll out the candidate with the availability and connection envelope agreed
   with task 04. A proposed `maxUnavailable: 0` and bounded surge still require
   capacity for new and draining connections. The bundled 48-replica, 25% surge
   configuration can exceed `max_connections=400`; this patch does not change it.
   New startup applies `006` through Alembic after a short index validation.
   A missing index on a populated table fails startup with a prebuild instruction
   instead of starting a long build under the five-minute startup timeout.

   With `TASK03_CANDIDATE_IMAGE` set to the reviewed digest and the rollout
   strategy already reconciled by task 04:

   ```bash
   kubectl --context gke-europe-west1 --namespace mellum set image \
     deployment/idegym-orchestrator "orchestrator=$TASK03_CANDIDATE_IMAGE"
   kubectl --context gke-europe-west1 --namespace mellum \
     rollout status deployment/idegym-orchestrator --timeout=10m
   ```

7. Check readiness, database pool waits and latency, and the client stop/finish
   result sets using the rollout's disposable clients. Retain the staged-load
   acceptance gates from task 09. This change does not establish production
   sandbox capacity or resolve `/dev/shm`, retention, Redis, or Kueue failures.

The existing image remains startup-compatible throughout prebuilding because
the schema revision stays `005`. Once `006` is recorded, already-running old
processes continue to serve, but an old image restarting its Alembic runner does
not recognize `006`. The rollback procedure below handles that metadata boundary.
Do not rely on an image-only rollback.

## Monitor and recover the index build

Run these queries on a separate connection, in a read-only transaction with a
short statement timeout:

```sql
SELECT p.pid, p.phase, p.lockers_total, p.lockers_done, p.current_locker_pid,
       p.blocks_total, p.blocks_done, p.tuples_total, p.tuples_done,
       a.wait_event_type, a.wait_event, pg_blocking_pids(p.pid) AS blockers
FROM pg_stat_progress_create_index p JOIN pg_stat_activity a USING (pid)
WHERE p.relid = 'public.servers'::regclass;

SELECT pid, application_name, state, now() - xact_start AS transaction_age,
       backend_xmin, wait_event_type, wait_event
FROM pg_stat_activity
WHERE datname = current_database() AND xact_start IS NOT NULL
ORDER BY xact_start;

SELECT i.indisvalid, i.indisready, i.indislive, pg_get_indexdef(i.indexrelid)
FROM pg_index i
WHERE i.indexrelid = to_regclass('public.ix_servers_client_id');

SELECT temp_files, temp_bytes, deadlocks
FROM pg_stat_database WHERE datname = current_database();
```

Monitor PostgreSQL CPU, storage read/write latency and throughput, checkpoint
activity, WAL growth, temp-space usage, available volume bytes, and request/pool
waits alongside these queries. Use counter deltas. A concurrent build scans the
table twice, can sort into temporary files, and waits for older writers and
snapshots. It allows normal DML but competes for I/O and conflicts with some DDL.
Run one schema operation at a time; do not overlap task 05's index construction.
Do not disable transaction timeouts or increase maintenance memory without the
task 04 resource review.

If application health or disk headroom crosses the rollout's agreed limits,
cancel only the observed build backend, identified by its progress row and
`application_name = 'idegym-server-client-index'`. Keep the old deployment and
revision `005` in place. A cancelled build can leave an invalid index consuming
disk and write work. After the backend has exited and transaction blockers are
resolved, run:

```bash
/opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index build --repair-invalid
/opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index check
```

The repair command retains a valid index, concurrently drops an invalid index
only when its definition matches this migration, and recreates it concurrently.
It also recovers an interrupted concurrent drop. An unexpected same-name object
fails validation and requires inspection; `IF NOT EXISTS` alone is insufficient.
Each repair uses the same advisory lock and leaves the schema revision unchanged.
The offline Alembic SQL includes the required transaction boundary, but cannot
perform these online catalog checks; use the prebuild command for production.

## Roll back the application

Before promotion, cancellation or a failed build requires no schema-version
rollback. Inspect and repair any invalid index before a later attempt.

After promotion to `006`, keep the index. Its downgrade performs no index drop
and changes no server data. Coordinate the following sequence with the rollout
owner so a starting candidate cannot advance the revision again:

1. Pause further candidate rollout. Keep existing ready pods serving and retain
   the agreed surge/availability budget for the rollback.
2. Open an administrative `psql` session with autocommit and acquire
   `SELECT pg_advisory_lock(42239);`. Keep this session connected through step 5.
   Application startup skips migration when this lock is held and waits for its
   expected schema version; it cannot promote the version during rollback.
3. Require revision `006`, or `005` if this rollback already completed. If task 05's `007` is
   present, complete its reviewed rollback to `006` first. In a separate
   candidate-image process with the same `POSTGRES_*` environment, run:

   ```python
   import os
   from pathlib import Path
   from alembic import command
   from alembic.config import Config
   from sqlalchemy import URL, create_engine, text
   from idegym.orchestrator.migrations import server_client_index

   url = URL.create(
       "postgresql+asyncpg",
       username=os.environ["POSTGRES_USER"],
       password=os.environ.get("POSTGRES_PASSWORD"),
       host=os.environ["POSTGRES_HOST"],
       port=int(os.environ.get("POSTGRES_PORT", "5432")),
       database=os.environ["POSTGRES_DB"],
   )
   engine = create_engine(url.set(drivername="postgresql+psycopg2"))
   with engine.connect() as connection:
       current = connection.scalar(text("SELECT version_num FROM alembic_version"))
       if current not in {"005", "006"}:
           raise RuntimeError(f"Coordinate rollback of revision {current} before reverting 006")
   engine.dispose()
   config = Config(str(Path(server_client_index.__file__).parents[1] / "alembic.ini"))
   config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
   command.downgrade(config, "005")
   ```

4. Roll out the previous `reconcile-ef6a8bf` image while retaining the advisory
   lock. Old-image startups observe their expected revision `005`; running
   candidate processes remain compatible with the retained index and data.
5. Require the previous image's desired replicas to be available and no candidate
   replicas to remain before releasing `SELECT pg_advisory_unlock(42239);` in the
   administrative session. Confirm revision `005` and normal service behavior.

Index removal is optional maintenance after recovery:
`DROP INDEX CONCURRENTLY IF EXISTS public.ix_servers_client_id;` in autocommit.
It reintroduces the unindexed lookup cost and must not run during incident
rollback. No server rows are removed or rewritten by this migration.

## Handoff requirements

| Owner | Interface and integration requirement |
| --- | --- |
| Task 05 | Chain retention `007` after client index `006` after `005`. Include `server_client_index.py` with the three `006_*` files. Preserve the query/helper imports and `IdeGYMServer.__table_args__` index while merging its `AsyncOperation` changes. Serialize prebuild jobs using lock `42239`; no retention activation is included here. |
| Task 04 | Budget prebuild and Alembic connections, surge and draining processes, PostgreSQL I/O/WAL/temp space, and timeout policy. Preserve the populated-table startup guard. Own `/dev/shm`, pool lifecycle, and Deployment availability changes. |
| Task 08 | Build the orchestrator from the combined reviewed source and record its digest. Task 03 alone is compatible with the existing watcher because it connects without running migrations. For the combined retention rollout, build orchestrator and watcher from the same merged source and follow task 05's schema/configuration requirements. Keep quota accounting's `FINISHED` status. |
| Task 09 | Recheck plans with actual client selectivity, custom/generic prepared plans, HTTP latency, pool waits, and the staged sandbox workload. Local SQL results do not establish 9,000-pod reliability. |

Shared-file edits in this patch are limited to the live query/imports in
`database.py`, the live helper in `helpers.py`, the server index in `models.py`,
and the optional disposable-database fixture in `integration-tests/database/conftest.py`.
Task 05 proposes `007` after `006`; no migration numbering conflict is expected.
Cross-task notification was unavailable after the initial ownership message,
so this document records the interface requirements for integration review.
