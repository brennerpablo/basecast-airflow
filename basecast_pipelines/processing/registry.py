"""``dataset_registry``: what ``basecast datasets`` prints, published to Postgres for the API's /data pages.

One row per (source, dataset); a SQL dataset declared by several modules appears once per module.
``basecast registry`` publishes every source; each ``process`` run refreshes its own source's rows."""

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
"""


def rows_for(source_id: str, module: ModuleType) -> list[tuple]:
    rows = [
        (
            source_id, d.name, d.target, d.mode, d.version, d.description, list(d.key) or None,
            d.partition[0] if d.partition else None, list(d.cluster) or None,
        )
        for d in getattr(module, "DATASETS", [])
    ]
    rows += [
        (source_id, d.name, "postgres", "sql", None, d.description, None, None, None)
        for d in getattr(module, "SQL_DATASETS", [])
    ]
    return rows


def publish(conn: psycopg.Connection, modules: Mapping[str, ModuleType] | None = None) -> int:
    """Upsert the registry rows of ``modules`` (source id → parser module; every processable source when
    None) and drop their rows for datasets that no longer exist. Returns the number of rows written."""
    everything = modules is None
    if everything:
        modules = {s: get_parser(s) for s in processable_sources()}
    sources = list(modules)
    rows = [r for s, m in modules.items() for r in rows_for(s, m)]
    with conn.transaction():
        conn.execute(DDL)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO dataset_registry (source_id, dataset, target, mode, version, description,
                                              key_columns, partition_field, cluster_fields, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (source_id, dataset) DO UPDATE SET
                    target = EXCLUDED.target, mode = EXCLUDED.mode, version = EXCLUDED.version,
                    description = EXCLUDED.description, key_columns = EXCLUDED.key_columns,
                    partition_field = EXCLUDED.partition_field, cluster_fields = EXCLUDED.cluster_fields,
                    updated_at = EXCLUDED.updated_at
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
