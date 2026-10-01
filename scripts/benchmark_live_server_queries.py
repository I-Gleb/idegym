"""Compare client lookup plans on a populated, disposable local PostgreSQL database."""

import argparse
import json
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from alembic import command
from alembic.config import Config
from idegym.orchestrator.migrations import server_client_index as index
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

_CLIENTS = 1000
_LIVE_SERVERS = 9000
_FINISHED_SERVERS = 1000
_COLUMNS = "id, generated_name"
_PREDICATE = "client_id = :client_id AND availability IN ('ALIVE', 'REUSED')"


def nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from nodes(child)


def explain(connection, query, parameters):
    result = connection.scalar(
        text(f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON, TIMING OFF) {query}"), parameters
    )
    plan = result[0]
    scans = [
        node for node in nodes(plan["Plan"]) if node["Node Type"] in {"Seq Scan", "Index Scan", "Bitmap Heap Scan"}
    ]
    return {
        "execution_ms": plan["Execution Time"],
        "rows_returned": plan["Plan"]["Actual Rows"],
        "heap_row_visits": sum(
            (node["Actual Rows"] + node.get("Rows Removed by Filter", 0) + node.get("Rows Removed by Index Recheck", 0))
            * node["Actual Loops"]
            for node in scans
        ),
        "shared_blocks": plan["Plan"].get("Shared Hit Blocks", 0) + plan["Plan"].get("Shared Read Blocks", 0),
        "plan": plan,
    }


def measure(connection, query, parameters, repeats):
    samples = [explain(connection, query, parameters) for _ in range(repeats)]
    return {
        "query": query,
        "parameters": {key: str(value) for key, value in parameters.items()},
        "median_execution_ms": statistics.median(sample["execution_ms"] for sample in samples),
        "samples": samples,
    }


def collect_plans(connection, repeats):
    probes = {"typical": UUID(int=500), "history_heavy": UUID(int=1), "missing": UUID(int=1001)}
    measured = {}
    for label, client_id in probes.items():
        parameters = {"client_id": client_id}
        measured[label] = {
            "history": measure(
                connection, "SELECT * FROM public.servers WHERE client_id = :client_id", parameters, repeats
            ),
            "live": measure(
                connection, f"SELECT {_COLUMNS} FROM public.servers WHERE {_PREDICATE}", parameters, repeats
            ),
        }

    # Compare parameterized statuses with the fixed SQL statuses used by the live API.
    connection.execute(text("SET plan_cache_mode = force_generic_plan"))
    connection.execute(
        text(
            f"PREPARE live_lookup(uuid, varchar, varchar) AS SELECT {_COLUMNS} FROM public.servers "
            "WHERE client_id = $1 AND availability IN ($2, $3)"
        )
    )
    connection.execute(
        text(
            f"PREPARE live_fixed_statuses(uuid) AS SELECT {_COLUMNS} FROM public.servers "
            "WHERE client_id = $1 AND availability IN ('ALIVE', 'REUSED')"
        )
    )
    try:
        for label, client_id in probes.items():
            measured[label]["live_generic_bound_statuses"] = measure(
                connection, f"EXECUTE live_lookup('{client_id}'::uuid, 'ALIVE', 'REUSED')", {}, repeats
            )
            measured[label]["live_generic_fixed_statuses"] = measure(
                connection, f"EXECUTE live_fixed_statuses('{client_id}'::uuid)", {}, repeats
            )
    finally:
        connection.execute(text("DEALLOCATE live_lookup"))
        connection.execute(text("DEALLOCATE live_fixed_statuses"))
        connection.execute(text("SET plan_cache_mode = auto"))
    return measured


def populate(connection, rows):
    connection.execute(
        text(
            "INSERT INTO clients (id, name) SELECT lpad(to_hex(g), 32, '0')::uuid, 'client-' || g FROM generate_series(1, :clients) AS g"
        ),
        {"clients": _CLIENTS},
    )
    started = time.perf_counter()
    connection.execute(
        text("""
        INSERT INTO servers (
            client_id, client_name, server_name, generated_name, namespace,
            availability, cpu, ram, image_tag, pod_ip, pod_manifest, details
        )
        SELECT lpad(to_hex(CASE
                   WHEN g <= :history OR (g > :rows - :live AND g <= :rows - :live / 2) THEN 1
                   ELSE 2 + g % (:clients - 1) END), 32, '0')::uuid,
               'benchmark-client', 'sandbox', 'benchmark-sandbox-' || g, 'test',
               CASE WHEN g > :rows - :live THEN CASE WHEN g % 3 = 0 THEN 'REUSED' ELSE 'ALIVE' END
                    WHEN g > :rows - :live - :finished THEN 'FINISHED' ELSE 'KILLED' END,
               2.0, 5.0, 'example.invalid/sandbox:synthetic', '127.0.0.1',
               jsonb_build_object('metadata', jsonb_build_object('name', 'sandbox-' || g),
                                  'spec', jsonb_build_object('payload', repeat(md5(g::text), 128))),
               repeat(md5((g * 7)::text), 128)
        FROM generate_series(1, :rows) AS g
    """),
        {"rows": rows, "history": rows // 5, "live": _LIVE_SERVERS, "finished": _FINISHED_SERVERS, "clients": _CLIENTS},
    )
    connection.execute(text("ANALYZE public.servers"))
    return time.perf_counter() - started


def fetch_comparison(connection):
    parameters = {"client_id": UUID(int=500)}
    start = time.perf_counter()
    history = (
        connection.execute(text("SELECT * FROM public.servers WHERE client_id = :client_id"), parameters)
        .mappings()
        .all()
    )
    old_live = {(row["id"], row["generated_name"]) for row in history if row["availability"] in {"ALIVE", "REUSED"}}
    old_ms = (time.perf_counter() - start) * 1000
    payload_characters = sum(len(row["details"]) + len(json.dumps(row["pod_manifest"])) for row in history)
    start = time.perf_counter()
    live = connection.execute(text(f"SELECT {_COLUMNS} FROM public.servers WHERE {_PREDICATE}"), parameters).all()
    new_ms = (time.perf_counter() - start) * 1000
    assert set(live) == old_live
    return {
        "client": str(parameters["client_id"]),
        "history_rows_materialized": len(history),
        "live_rows_materialized": len(live),
        "history_payload_characters_materialized": payload_characters,
        "live_payload_characters_materialized": 0,
        "history_fetch_and_filter_ms": old_ms,
        "live_fetch_ms": new_ms,
        "results_equal": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=600_000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.rows < 100_000 or args.repeats < 1:
        parser.error("Use at least 100000 rows and one repeat")
    raw_url = os.environ.get("IDEGYM_TEST_DATABASE_URL")
    if not raw_url:
        parser.error("Set IDEGYM_TEST_DATABASE_URL to a disposable local PostgreSQL instance")
    url = make_url(raw_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or not (url.database or "").startswith("idegym_test_"):
        parser.error("The benchmark requires localhost and an idegym_test_* database")

    database = f"idegym_test_benchmark_{uuid4().hex}"
    admin = create_engine(url.set(drivername="postgresql+psycopg2"), isolation_level="AUTOCOMMIT", poolclass=NullPool)
    engine = create_engine(
        url.set(drivername="postgresql+psycopg2", database=database), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    with admin.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{database}"'))
    try:
        config = Config(str(Path(index.__file__).parents[1] / "alembic.ini"))
        config.set_main_option(
            "sqlalchemy.url",
            url.set(drivername="postgresql+asyncpg", database=database)
            .render_as_string(hide_password=False)
            .replace("%", "%%"),
        )
        command.upgrade(config, "005")
        with engine.connect() as connection:
            connection.execute(text("SET statement_timeout = '5min'"))
            connection.execute(text("SET lock_timeout = '5s'"))
            result = {
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "postgres_version": connection.scalar(text("SELECT version()")),
                "rows": args.rows,
                "clients": _CLIENTS,
                "live_servers": _LIVE_SERVERS,
                "finished_servers": _FINISHED_SERVERS,
                "repeats": args.repeats,
                "settings": dict(
                    connection.execute(
                        text(
                            "SELECT name, setting FROM pg_settings WHERE name IN ('shared_buffers', 'effective_cache_size', 'work_mem', 'maintenance_work_mem', 'max_parallel_workers_per_gather', 'random_page_cost', 'seq_page_cost')"
                        )
                    ).all()
                ),
            }
            print("Populating synthetic server history", flush=True)
            result["populate_seconds"] = populate(connection, args.rows)
            result["table_bytes"] = connection.scalar(text("SELECT pg_table_size('public.servers')"))
            result["status_counts"] = dict(
                connection.execute(text("SELECT availability, count(*) FROM servers GROUP BY availability")).all()
            )
            print("Measuring plans before the client index", flush=True)
            result["before"] = collect_plans(connection, args.repeats)
            started = time.perf_counter()
            index.ensure_index(connection, allow_populated_build=True)
            result["index_build_seconds"] = time.perf_counter() - started
            result["index_bytes"] = connection.scalar(text("SELECT pg_relation_size('public.ix_servers_client_id')"))
            result["index_state"] = dict(index.index_state(connection))
            print("Measuring plans after the client index", flush=True)
            result["after"] = collect_plans(connection, args.repeats)
            result["fetch_comparison"] = fetch_comparison(connection)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(f"Saved query plans to {args.output}", flush=True)
    finally:
        engine.dispose()
        with admin.connect() as connection:
            connection.execute(text(f'DROP DATABASE "{database}" WITH (FORCE)'))
        admin.dispose()


if __name__ == "__main__":
    main()
