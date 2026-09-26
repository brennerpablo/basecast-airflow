"""Read-only access to the ``basecast`` database for the models, as ``basecast_reader``.

The models only read: marts are written later by the pipelines (task A0), never from here. The DSN comes
from ``BASECAST_READER_DSN`` or is built for the Cloud SQL proxy on the Mac (``PG_HOST``/``PG_PORT``,
default 127.0.0.1:5439, as in ``deploy/gcp/README.md``); the password comes from ``PG_READER_PASSWORD``
or Secret Manager (``pg-basecast-reader-password``).
"""

from __future__ import annotations

import os
import subprocess
from functools import cache
from typing import Any

import polars as pl
import psycopg

PROJECT = "basecast-509812"
SECRET = "pg-basecast-reader-password"


@cache
def reader_dsn() -> str:
    if dsn := os.environ.get("BASECAST_READER_DSN"):
        return dsn
    password = os.environ.get("PG_READER_PASSWORD") or subprocess.run(
        ["gcloud", "secrets", "versions", "access", "latest", "--secret", SECRET, "--project", PROJECT],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = os.environ.get("PG_PORT", "5439")
    return f"host={host} port={port} dbname=basecast user=basecast_reader password={password}"


def read_sql(query: str, params: dict[str, Any] | None = None) -> pl.DataFrame:
    """Run one read-only query and return it as a DataFrame (the session itself is read-only)."""
    with psycopg.connect(reader_dsn(), application_name="basecast-models") as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description is None:
                return pl.DataFrame()
            columns = [c.name for c in cur.description]
            rows = cur.fetchall()
    return pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)
