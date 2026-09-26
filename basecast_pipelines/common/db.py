"""Postgres writes (the ``basecast`` database). Tables are created from the DataFrame schema; every write
goes through COPY into a temporary staging table and then one transaction that deletes the write's scope
and inserts the new rows, so readers never see a half-loaded table and re-running never duplicates."""

from __future__ import annotations

import io
import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import Literal

import polars as pl
import psycopg
from psycopg import sql

log = logging.getLogger(__name__)

WriteMode = Literal["replace", "by_file", "by_key"]
COPY_CHUNK_ROWS = 200_000
SRID = 4326


class SchemaMismatchError(Exception):
    """An existing table has a column whose type differs from the new data (reprocess to rebuild it)."""


def connect(url: str) -> psycopg.Connection:
    """Autocommit connection: each ``conn.transaction()`` block is one real transaction."""
    return psycopg.connect(url, autocommit=True, application_name="basecast-pipelines")


def pg_type(dtype: pl.DataType) -> str:
    """Postgres type for a polars dtype, spelled as ``format_type()`` reports it."""
    if dtype == pl.Boolean:
        return "boolean"
    if dtype in (pl.Int8, pl.Int16, pl.UInt8):
        return "smallint"
    if dtype in (pl.Int32, pl.UInt16):
        return "integer"
    if dtype in (pl.Int64, pl.UInt32):
        return "bigint"
    if dtype == pl.UInt64:
        return "numeric"
    if dtype in (pl.Float32, pl.Float64):
        return "double precision"
    if isinstance(dtype, pl.Decimal):
        return "numeric"
    if dtype == pl.Date:
        return "date"
    if isinstance(dtype, pl.Datetime):
        return "timestamp with time zone" if dtype.time_zone else "timestamp without time zone"
    if dtype == pl.Time:
        return "time without time zone"
    if dtype in (pl.String, pl.Null) or isinstance(dtype, (pl.Categorical, pl.Enum)):
        return "text"
    raise TypeError(f"unsupported column type for Postgres: {dtype} (encode nested values as JSON text)")


def column_types(df: pl.DataFrame, geometry: Mapping[str, int] | None = None) -> dict[str, str]:
    geometry = geometry or {}
    return {
        name: f"geometry(Geometry,{SRID})" if name in geometry else pg_type(dtype)
        for name, dtype in df.schema.items()
    }


def existing_columns(conn: psycopg.Connection, table: str) -> dict[str, str] | None:
    rows = conn.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (f"public.{table}",),
    ).fetchall()
    return {name: typ for name, typ in rows} or None


def ensure_table(
    conn: psycopg.Connection,
    table: str,
    columns: Mapping[str, str],
    *,
    indexes: Iterable[Sequence[str]] = (),
) -> None:
    """Create the table, or add the columns it lacks. A type change raises ``SchemaMismatchError``."""
    current = existing_columns(conn, table)
    if current is None:
        conn.execute(
            sql.SQL("CREATE TABLE {} ({})").format(
                sql.Identifier(table),
                sql.SQL(", ").join(
                    sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(t)) for c, t in columns.items()
                ),
            )
        )
    else:
        for name, typ in columns.items():
            if name not in current:
                conn.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                        sql.Identifier(table), sql.Identifier(name), sql.SQL(typ)
                    )
                )
            elif current[name].lower() != typ.lower():
                raise SchemaMismatchError(f"{table}.{name}: table has {current[name]}, data has {typ}")
    for cols in indexes:
        conn.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(f"{table}__{'__'.join(cols)}_idx"[:63]),
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, cols)),
            )
        )


def _csv_ready(df: pl.DataFrame) -> pl.DataFrame:
    """Datetimes as explicit ISO text (UTC offset for aware ones), so COPY never guesses."""
    exprs = []
    for name, dtype in df.schema.items():
        if isinstance(dtype, pl.Datetime):
            col = pl.col(name)
            if dtype.time_zone:
                exprs.append((col.dt.convert_time_zone("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%.6f") + "+00").alias(name))
            else:
                exprs.append(col.dt.strftime("%Y-%m-%d %H:%M:%S%.6f").alias(name))
    return df.with_columns(exprs) if exprs else df


def copy_frame(conn: psycopg.Connection, table: str, df: pl.DataFrame) -> None:
    """COPY a DataFrame into ``table`` as CSV: nulls are unquoted empty fields, empty strings are ``""``."""
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, df.columns))
    )
    with conn.cursor().copy(statement) as copy:
        for offset in range(0, df.height, COPY_CHUNK_ROWS):
            buffer = io.BytesIO()
            _csv_ready(df.slice(offset, COPY_CHUNK_ROWS)).write_csv(buffer, include_header=False, null_value="")
            copy.write(buffer.getvalue())


def write_table(
    conn: psycopg.Connection,
    table: str,
    df: pl.DataFrame,
    *,
    mode: WriteMode,
    scope_files: Sequence[str] = (),
    key: Sequence[str] = (),
    geometry: Mapping[str, int] | None = None,
    recreate: bool = False,
) -> int:
    """Replace the write's scope with ``df`` in one transaction (the caller commits).

    ``replace``: the whole table. ``by_file``: rows whose ``source_file`` is in ``scope_files``.
    ``by_key``: rows sharing a ``key`` with the new data. ``geometry`` maps GeoJSON text columns to their
    SRID; they are stored as PostGIS geometries in EPSG:4326."""
    geometry = geometry or {}
    if mode == "by_key":
        if not key:
            raise ValueError(f"{table}: by_key needs key columns")
        df = df.unique(subset=list(key), keep="last", maintain_order=True)
    columns = column_types(df, geometry)
    if recreate:
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table)))
    indexes = [["source_file"]] if mode == "by_file" else [list(key)] if mode == "by_key" else []
    ensure_table(conn, table, columns, indexes=indexes)

    stage = f"_stage_{table}"[:63]
    conn.execute(
        sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
            sql.Identifier(stage),
            sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL("text" if c in geometry else t))
                for c, t in columns.items()
            ),
        )
    )
    copy_frame(conn, stage, df)

    target = sql.Identifier(table)
    if mode == "replace":
        conn.execute(sql.SQL("DELETE FROM {}").format(target))
    elif mode == "by_file":
        conn.execute(sql.SQL("DELETE FROM {} WHERE source_file = ANY(%s)").format(target), (list(scope_files),))
    else:
        match = sql.SQL(" AND ").join(
            sql.SQL("t.{c} = s.{c}").format(c=sql.Identifier(c)) for c in key
        )
        conn.execute(sql.SQL("DELETE FROM {} t USING {} s WHERE {}").format(target, sql.Identifier(stage), match))

    select = []
    for c in columns:
        if c in geometry:
            src = geometry[c]
            expr = sql.SQL("ST_SetSRID(ST_GeomFromGeoJSON({}), {})").format(sql.Identifier(c), sql.Literal(src))
            if src != SRID:
                expr = sql.SQL("ST_Transform({}, {})").format(expr, sql.Literal(SRID))
            select.append(expr)
        else:
            select.append(sql.Identifier(c))
    conn.execute(
        sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {}").format(
            target,
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(select),
            sql.Identifier(stage),
        )
    )
    log.info("%s: %s %s rows", table, mode, df.height)
    return df.height


# --- bookkeeping tables ------------------------------------------------------------------------------

ETL_RUN_DDL = """
CREATE TABLE IF NOT EXISTS etl_run (
    run_id text PRIMARY KEY,
    source text NOT NULL,
    stage text NOT NULL,
    dag_id text,
    task_id text,
    airflow_run_id text,
    try_number integer,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    duration_s double precision,
    status text NOT NULL,
    rows bigint,
    files bigint,
    files_skipped bigint,
    bytes bigint,
    error text,
    events jsonb,
    params jsonb
);
CREATE INDEX IF NOT EXISTS etl_run__source_started_idx ON etl_run (source, started_at DESC);
CREATE INDEX IF NOT EXISTS etl_run__status_started_idx ON etl_run (status, started_at DESC);
"""

LAKE_PROCESSED_DDL = """
CREATE TABLE IF NOT EXISTS lake_processed (
    dataset text NOT NULL,
    raw_key text NOT NULL,
    sha256 text NOT NULL,
    version integer NOT NULL,
    rows bigint,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, raw_key)
);
"""


def ensure_bookkeeping(conn: psycopg.Connection) -> None:
    conn.execute(ETL_RUN_DDL)
    conn.execute(LAKE_PROCESSED_DDL)
