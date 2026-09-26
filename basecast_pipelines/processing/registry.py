"""``dataset_registry``: what ``basecast datasets`` prints, published to Postgres for the API's /data pages.

One row per (source, dataset); a SQL dataset declared by several modules appears once per module.
``basecast registry`` publishes every source; each ``process`` run refreshes its own source's rows. The marts
(``basecast marts build``) publish under ``source_id = "marts"``, one row per mart table that exists, with
``inputs``: the tables the mart reads, named as ``/tables`` shows them (the app's /data/flow lineage)."""

from __future__ import annotations

from collections.abc import Mapping
from types import ModuleType

import psycopg

from basecast_pipelines.parsers import get_parser, processable_sources

DDL = """
CREATE TABLE IF NOT EXISTS dataset_registry (
    source_id text NOT NULL,
    dataset text NOT NULL,
    target text NOT NULL,
    mode text NOT NULL,
    version integer,
    description text,
    key_columns text[],
    partition_field text,
    cluster_fields text[],
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, dataset)
);
ALTER TABLE dataset_registry ADD COLUMN IF NOT EXISTS inputs text[];
"""
MARTS_SOURCE = "marts"


def rows_for(source_id: str, module: ModuleType) -> list[tuple]:
    rows = [
        (
            source_id, d.name, d.target, d.mode, d.version, d.description, list(d.key) or None,
            d.partition[0] if d.partition else None, list(d.cluster) or None, None,
        )
        for d in getattr(module, "DATASETS", [])
    ]
    rows += [
        (source_id, d.name, "postgres", "sql", None, d.description, None, None, None, None)
        for d in getattr(module, "SQL_DATASETS", [])
    ]
    return rows


def mart_rows(conn: psycopg.Connection) -> list[tuple]:
    """Registry rows of the marts whose table exists (a mart never built, or never passing its checks, has none)."""
    from basecast_pipelines.marts.catalog import MARTS
    from basecast_pipelines.marts.core import existing_marts

    built = set(existing_marts(conn, list(MARTS)))
    return [
        (MARTS_SOURCE, m.name, "postgres", "replace", m.version, m.description, list(m.key) or None, None, None,
         list(m.inputs) or None)
        for m in MARTS.values()
        if m.name in built
    ]


def publish(conn: psycopg.Connection, modules: Mapping[str, ModuleType] | None = None) -> int:
    """Upsert the registry rows of ``modules`` (source id → parser module; every processable source, plus the
    built marts, when None) and drop their rows for datasets that no longer exist. Returns the number of rows
    written."""
    everything = modules is None
    if everything:
        modules = {s: get_parser(s) for s in processable_sources()}
    rows = [r for s, m in modules.items() for r in rows_for(s, m)]
    if everything:
        rows += mart_rows(conn)
    return _write(conn, rows, None if everything else list(modules))


def publish_marts(conn: psycopg.Connection) -> int:
    """Refresh the ``marts`` rows: one per mart table that exists."""
    return _write(conn, mart_rows(conn), [MARTS_SOURCE])


def _write(conn: psycopg.Connection, rows: list[tuple], sources: list[str] | None) -> int:
    """Upsert ``rows``; drop the other rows of ``sources`` (every other row when None)."""
    everything = sources is None
    with conn.transaction():
        conn.execute(DDL)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO dataset_registry (source_id, dataset, target, mode, version, description,
                                              key_columns, partition_field, cluster_fields, inputs, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (source_id, dataset) DO UPDATE SET
                    target = EXCLUDED.target, mode = EXCLUDED.mode, version = EXCLUDED.version,
                    description = EXCLUDED.description, key_columns = EXCLUDED.key_columns,
                    partition_field = EXCLUDED.partition_field, cluster_fields = EXCLUDED.cluster_fields,
                    inputs = EXCLUDED.inputs, updated_at = EXCLUDED.updated_at
                """,
                rows,
            )
        keep = [(r[0], r[1]) for r in rows]
        if everything:
            conn.execute(
                "DELETE FROM dataset_registry WHERE (source_id, dataset) NOT IN (SELECT * FROM unnest(%s::text[], %s::text[]))",
                ([k[0] for k in keep], [k[1] for k in keep]),
            )
        else:
            conn.execute(
                "DELETE FROM dataset_registry WHERE source_id = ANY(%s) "
                "AND (source_id, dataset) NOT IN (SELECT * FROM unnest(%s::text[], %s::text[]))",
                (sources, [k[0] for k in keep], [k[1] for k in keep]),
            )
    return len(rows)
