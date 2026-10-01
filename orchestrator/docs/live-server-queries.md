# Deploy the client lookup index

Migration `006` adds `ix_servers_client_id` on `servers(client_id)` without
rewriting server rows. Populated databases require prebuilding before the new
orchestrator starts; empty databases can build the index during migration.

## Prebuild and deploy

1. Confirm schema revision `005`, serving replicas, and database disk/I/O
   headroom. Record the previous image digest and review the rollout connection
   budget, including surge and draining processes. Serialize schema operations.
2. In a one-off process using the candidate image, supply the service's
   `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_DB`, and
   `POSTGRES_PASSWORD` when required. Run:

   ```bash
   /opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index build \
     --lock-timeout-seconds 5 --statement-timeout-seconds 1800
   /opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index check
   ```

   Review these timeout values against the database workload. The build holds
   advisory lock `42239`, uses autocommit for `CREATE INDEX CONCURRENTLY`, and
   leaves Alembic at `005`, allowing the old application to keep serving.
3. Monitor `pg_stat_progress_create_index`, older transactions and blockers in
   `pg_stat_activity`, database CPU/I/O, WAL/temp space, free disk, and request
   latency/pool waits. A concurrent build permits reads and writes but scans
   the table twice and waits for older transactions. The helper's
   `application_name` is `idegym-server-client-index`.
4. Require a successful `check` and schema revision `005` before rollout. The
   check verifies the index definition and `indisvalid`, `indisready`, and
   `indislive`. Deploy the candidate within the reviewed connection budget;
   startup validates the index and advances to `006`.
5. Check replica readiness, schema `006`, client stop/finish behavior, and
   database latency/pool waits. The existing watcher remains compatible with
   this migration. Coordinate any later migrations and their image requirements.

## Recover an interrupted build

If latency exceeds the agreed maximum or free disk falls below the agreed
minimum, cancel only the build backend identified by its progress row and
application name. Keep the old
image and schema `005` until the index is valid. After that backend exits and
transaction blockers are resolved, run in the candidate image:

```bash
/opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index build --repair-invalid
/opt/orchestrator/.venv/bin/python -m idegym.orchestrator.migrations.server_client_index check
```

Repair retains a valid index and concurrently drops/recreates an invalid index
only when its definition matches this migration. An unexpected definition
requires inspection. These commands preserve the Alembic revision.

## Roll back the application

An old image cannot start against revision `006`. Its downgrade retains the
index and changes no server data. To prevent candidate startups from advancing
the revision again:

1. Pause candidate rollout and retain ready replicas while replacing the image.
2. In an administrative `psql` session with autocommit, run
   `SELECT pg_advisory_lock(42239);` and keep the session open through step 4.
3. From a separate candidate-image process with the same `POSTGRES_*` environment,
   downgrade to `005` using the script below. If a later revision is present,
   complete its reviewed rollback to `006` first.
4. Restore the previous image and wait until its required replicas are ready and
   no candidate replicas remain. Then run `SELECT pg_advisory_unlock(42239);` in
   the administrative session and confirm revision `005` and service health.

```bash
/opt/orchestrator/.venv/bin/python - <<'PY'
import os
from pathlib import Path
from alembic import command
from alembic.config import Config
from sqlalchemy import URL, create_engine, text
from idegym.orchestrator.migrations import server_client_index

url = URL.create(
    "postgresql+asyncpg", username=os.environ["POSTGRES_USER"],
    password=os.environ.get("POSTGRES_PASSWORD"), host=os.environ["POSTGRES_HOST"],
    port=int(os.environ.get("POSTGRES_PORT", "5432")), database=os.environ["POSTGRES_DB"],
)
engine = create_engine(url.set(drivername="postgresql+psycopg2"))
with engine.connect() as connection:
    current = connection.scalar(text("SELECT version_num FROM alembic_version"))
    if current not in {"005", "006"}:
        raise RuntimeError(f"Roll back revision {current} before reverting 006")
engine.dispose()
config = Config(str(Path(server_client_index.__file__).parents[1] / "alembic.ini"))
config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
command.downgrade(config, "005")
PY
```
