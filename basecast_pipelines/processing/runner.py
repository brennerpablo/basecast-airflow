"""Run a source's parsers: pick the raw files each dataset still has to process, parse them and write the
tables, inside one ``etl_run`` (stage ``process``).

What was processed is recorded in the ``lake_processed`` table (dataset, raw key, sha256, parser
version), so a scheduled run only parses new raw files, and bumping a dataset's ``version`` reprocesses
everything it read. For Postgres targets the state is written in the same transaction as the rows."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import polars as pl

from basecast_pipelines.common.db import SchemaMismatchError, connect, delete_files, ensure_bookkeeping, write_table
from basecast_pipelines.common.etl_run import EtlRun
from basecast_pipelines.common.storage import Storage
from basecast_pipelines.config import Settings, local_today
from basecast_pipelines.processing.core import Dataset, RawFile, SqlDataset, iter_frames, list_raw_files
from basecast_pipelines.parsers import get_parser

log = logging.getLogger(__name__)

BATCH_ROWS = 2_000_000
BATCH_FILES = 100
PARQUET_PREFIX = "parquet"


@dataclass
class DatasetSummary:
    dataset: str
    target: str
    mode: str
    files: int = 0
    files_skipped: int = 0
    rows: int = 0
    skipped: bool = False
    files_pruned: int = 0
    schema: dict[str, str] = field(default_factory=dict)
    sample: pl.DataFrame | None = None
    rows_by_file: dict[str, int] = field(default_factory=dict)

    def event(self) -> dict:
        return {
            "dataset": self.dataset,
            "target": self.target,
            "mode": self.mode,
            "files": self.files,
            "files_skipped": self.files_skipped,
            "rows": self.rows,
            "skipped": self.skipped,
            "files_pruned": self.files_pruned,
        }


class ProcessingError(Exception):
    pass


def _with_lineage(df: pl.DataFrame, source_file: str | None) -> pl.DataFrame:
    if "source_file" not in df.columns:
        df = df.with_columns(pl.lit(source_file, dtype=pl.String).alias("source_file"))
    return df


def _stamp(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime("us", "UTC")).alias("ingested_at"))


def _for_parquet(df: pl.DataFrame) -> pl.DataFrame:
    """BigQuery has no unsigned integers."""
    unsigned = [n for n, t in df.schema.items() if t in (pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64)]
    return df.with_columns([pl.col(n).cast(pl.Int64) for n in unsigned]) if unsigned else df


def _write_parquet(storage: Storage, key: str, df: pl.DataFrame) -> str:
    tmp = storage.temp_path(".parquet")
    _for_parquet(df).write_parquet(tmp, compression="snappy", statistics=True)
    storage.put_file(key, tmp, overwrite=True)
    return storage.uri(key)


class _State:
    """``lake_processed`` rows for one dataset (empty when there is no database, e.g. dry runs)."""

    def __init__(self, conn, dataset: str) -> None:
        self.conn = conn
        self.dataset = dataset
        self.seen: dict[str, tuple[str, int]] = {}
        if conn is not None:
            rows = conn.execute(
                "SELECT raw_key, sha256, version FROM lake_processed WHERE dataset = %s", (dataset,)
            ).fetchall()
            self.seen = {k: (s, v) for k, s, v in rows}

    def is_current(self, f: RawFile, version: int) -> bool:
        return self.seen.get(f.key) == (f.sha256, version)

    def record(self, files: Sequence[RawFile], rows: dict[str, int], version: int, *, replace_all: bool) -> None:
        if self.conn is None:
            return
        if replace_all:
            self.conn.execute("DELETE FROM lake_processed WHERE dataset = %s", (self.dataset,))
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO lake_processed (dataset, raw_key, sha256, version, rows, processed_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (dataset, raw_key) DO UPDATE
                SET sha256 = EXCLUDED.sha256, version = EXCLUDED.version, rows = EXCLUDED.rows,
                    processed_at = EXCLUDED.processed_at
                """,
                [(self.dataset, f.key, f.sha256, version, rows.get(f.key, 0)) for f in files],
            )
        for f in files:
            self.seen[f.key] = (f.sha256, version)

    def forget(self, keys: Sequence[str]) -> None:
        if self.conn is None:
            return
        self.conn.execute(
            "DELETE FROM lake_processed WHERE dataset = %s AND raw_key = ANY(%s)", (self.dataset, list(keys))
        )
        for k in keys:
            self.seen.pop(k, None)


class _Writer:
    """Writes one dataset's batches to its target and records the processing state."""

    def __init__(self, ds: Dataset, *, storage: Storage, settings: Settings, conn, rebuild: bool) -> None:
        self.ds = ds
        self.storage = storage
        self.settings = settings
        self.conn = conn
        self.rebuild = rebuild
        self.bq = None
        if ds.target == "bigquery":
            if not settings.gcp_project:
                raise ProcessingError(f"{ds.name} goes to BigQuery: set GCP_PROJECT")
            from basecast_pipelines.common.bq import ensure_dataset
            from basecast_pipelines.common.gcp import bigquery_client

            self.bq = bigquery_client(settings.gcp_project, settings.bq_location)
            ensure_dataset(self.bq, settings.bq_dataset, settings.bq_location)

    def write(self, df: pl.DataFrame, files: Sequence[RawFile], rows: dict[str, int], state: _State,
              parquet_parts: dict[str, pl.DataFrame] | None = None) -> None:
        ds = self.ds
        if ds.mode == "by_key":
            for frame in (parquet_parts or {}).values() if ds.target == "bigquery" else [df]:
                nulls = frame.filter(pl.any_horizontal(pl.col(list(ds.key)).is_null())).height
                if nulls:
                    raise ProcessingError(f"{ds.name}: {nulls} rows with a null key {ds.key}; keys must be complete")
        replace_all = ds.mode == "replace"
        scope = [f.key for f in files]
        if ds.target == "postgres":
            self._write_postgres(df, scope, lambda: state.record(files, rows, ds.version, replace_all=replace_all))
        else:
            self._write_bigquery(df, files, parquet_parts)
            if self.conn is not None:
                with self.conn.transaction():
                    state.record(files, rows, ds.version, replace_all=replace_all)
        self.rebuild = False  # only the first write of a rebuild drops the table

    def _write_postgres(self, df: pl.DataFrame, scope: list[str], record) -> None:
        ds = self.ds
        recreate = self.rebuild
        for attempt in range(2):
            try:
                with self.conn.transaction():
                    write_table(
                        self.conn, ds.name, df, mode=ds.mode, scope_files=scope, key=ds.key,
                        geometry=ds.geometry, recreate=recreate,
                    )
                    record()
                return
            except SchemaMismatchError:
                if ds.mode != "replace" or attempt:
                    raise
                log.warning("%s: schema changed, recreating the table (replace mode)", ds.name)
                recreate = True

    def prune(self, keys: Sequence[str], state: _State) -> None:
        """Drop the rows of raw files the dataset no longer selects (superseded or corrected files)."""
        ds = self.ds
        if ds.target == "postgres":
            with self.conn.transaction():
                delete_files(self.conn, ds.name, keys)
                state.forget(keys)
            return
        from basecast_pipelines.common.bq import delete_files as bq_delete_files

        bq_delete_files(self.bq, dataset=self.settings.bq_dataset, table=ds.name, source_files=keys)
        with self.conn.transaction():
            state.forget(keys)

    def _write_bigquery(self, df: pl.DataFrame, files: Sequence[RawFile],
                        parquet_parts: dict[str, pl.DataFrame] | None) -> None:
        from basecast_pipelines.common.bq import load_parquet

        ds = self.ds
        uris = []
        if ds.mode == "replace":
            run_dt = local_today().isoformat()
            for i, offset in enumerate(range(0, max(df.height, 1), BATCH_ROWS)):
                key = f"{PARQUET_PREFIX}/{ds.name}/dt={run_dt}/part-{i:05d}.parquet"
                uris.append(_write_parquet(self.storage, key, df.slice(offset, BATCH_ROWS)))
        else:
            files_by_key = {f.key: f for f in files}
            for raw_key, part in (parquet_parts or {}).items():
                f = files_by_key[raw_key]
                key = f"{PARQUET_PREFIX}/{ds.name}/dt={f.dt.isoformat()}/part-{f.sha256[:16]}.parquet"
                uris.append(_write_parquet(self.storage, key, part))
        if not uris:
            return
        load_parquet(
            self.bq, dataset=self.settings.bq_dataset, table=ds.name, sources=uris, mode=ds.mode,
            key=ds.key, partition=ds.partition, cluster=ds.cluster, recreate=self.rebuild,
            order=[f.key for f in files],
        )


def _process_dataset(
    ds: Dataset,
    raw: list[RawFile],
    *,
    storage: Storage,
    settings: Settings,
    conn,
    reprocess: bool,
    rebuild: bool,
    dry_run: bool,
    max_files: int | None,
    complete: bool = False,
) -> DatasetSummary:
    """``complete``: every raw file of the source was listed (no dt window, no file cap), so by_file rows
    of files the dataset no longer selects can be pruned."""
    summary = DatasetSummary(ds.name, ds.target, ds.mode)
    files = ds.files(raw)
    if max_files is not None:
        files = files[:max_files]
    state = _State(conn, ds.name)
    writer = None if dry_run else _Writer(ds, storage=storage, settings=settings, conn=conn, rebuild=rebuild)

    if ds.mode == "replace":
        if files and not (reprocess or rebuild) and {f.key for f in files} == set(state.seen) and all(
            state.is_current(f, ds.version) for f in files
        ):
            summary.skipped, summary.files_skipped = True, len(files)
            return summary
        if ds.build is not None:
            df = ds.build(files)
            df = pl.DataFrame() if df is None else _with_lineage(df, files[-1].key if len(files) == 1 else None)
            rows = {f.key: 0 for f in files}
        else:
            frames, rows = [], {}
            for f, frame in iter_frames(ds, files):
                frames.append(_with_lineage(frame, f.key))
                rows[f.key] = frame.height
            df = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
        if ds.finalize is not None and df.height:
            df = ds.finalize(df)
        summary.files, summary.rows, summary.rows_by_file = len(files), df.height, rows
        if not df.height:
            log.warning("%s: no rows from %d files; table left as is", ds.name, len(files))
            return summary
        df = _stamp(df)
        summary.schema = {n: str(t) for n, t in df.schema.items()}
        summary.sample = df.head(10)
        if writer is not None:
            writer.write(df, files, rows, state)
        return summary

    pending = [f for f in files if reprocess or rebuild or not state.is_current(f, ds.version)]
    summary.files_skipped = len(files) - len(pending)
    batch_files: list[RawFile] = []
    batch_frames: dict[str, pl.DataFrame] = {}
    batch_rows = 0

    def flush() -> None:
        nonlocal batch_files, batch_frames, batch_rows
        if not batch_files:
            return
        rows = {k: v.height for k, v in batch_frames.items()}
        # BigQuery writes one Parquet per file; only Postgres needs the batch as one frame.
        if ds.target == "bigquery":
            df = pl.DataFrame({"_rows": [sum(rows.values())]}) if batch_frames else pl.DataFrame()
        else:
            df = pl.concat(list(batch_frames.values()), how="diagonal_relaxed") if batch_frames else pl.DataFrame()
        if writer is not None:
            if df.height or ds.mode == "by_file":
                if not df.height:
                    df = pl.DataFrame({"source_file": pl.Series([], dtype=pl.String)})
                writer.write(df, batch_files, rows, state, parquet_parts=batch_frames)
            else:
                with conn.transaction():
                    state.record(batch_files, rows, ds.version, replace_all=False)
        batch_files, batch_frames, batch_rows = [], {}, 0

    for f in pending:
        frame = ds.parse(f)  # type: ignore[misc]
        batch_files.append(f)
        summary.files += 1
        if frame is not None and frame.height:
            frame = _stamp(_with_lineage(frame, f.key))
            batch_frames[f.key] = frame
            batch_rows += frame.height
            summary.rows += frame.height
            summary.rows_by_file[f.key] = frame.height
            if not summary.schema:
                summary.schema = {n: str(t) for n, t in frame.schema.items()}
                summary.sample = frame.head(10)
            else:
                for n, t in frame.schema.items():
                    summary.schema.setdefault(n, str(t))
        if dry_run:
            batch_files, batch_frames, batch_rows = [], {}, 0
        elif batch_rows >= BATCH_ROWS or len(batch_files) >= BATCH_FILES:
            flush()
    if not dry_run:
        flush()
        if ds.mode == "by_file" and complete and conn is not None:
            stale = sorted(set(state.seen) - {f.key for f in files})
            if stale:
                log.info("%s: pruning rows of %d files no longer selected", ds.name, len(stale))
                writer.prune(stale, state)
                summary.files_pruned = len(stale)
    return summary


def _build_sql_dataset(conn, sd: SqlDataset) -> DatasetSummary:
    import psycopg
    from psycopg import sql

    summary = DatasetSummary(sd.name, "postgres", "sql")
    new = sql.Identifier(f"{sd.name}__new")
    try:
        with conn.transaction():
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(new))
            conn.execute(sql.SQL("CREATE TABLE {} AS ").format(new) + sql.SQL(sd.sql))
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(sd.name)))
            conn.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(new, sql.Identifier(sd.name)))
            for cols in sd.indexes:
                conn.execute(
                    sql.SQL("CREATE INDEX {} ON {} ({})").format(
                        sql.Identifier(f"{sd.name}__{'__'.join(cols)}_idx"[:63]),
                        sql.Identifier(sd.name),
                        sql.SQL(", ").join(map(sql.Identifier, cols)),
                    )
                )
            summary.rows = conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(sd.name))).fetchone()[0]
    except psycopg.errors.UndefinedTable as exc:
        log.warning("%s: an input table does not exist yet, skipped (%s)", sd.name, exc)
        summary.skipped = True
    return summary


def run_process(
    source_id: str,
    *,
    storage: Storage,
    settings: Settings,
    datasets: Sequence[str] | None = None,
    reprocess: bool = False,
    rebuild: bool = False,
    dry_run: bool = False,
    since: date | None = None,
    until: date | None = None,
    max_files: int | None = None,
) -> list[DatasetSummary]:
    """Process a source's raw files into its datasets. ``reprocess`` ignores the state (re-parse every
    selected file); ``rebuild`` also drops the tables first; ``dry_run`` parses without writing."""
    module = get_parser(source_id)
    chosen = [d for d in getattr(module, "DATASETS", []) if not datasets or d.name in datasets]
    derived = [d for d in getattr(module, "SQL_DATASETS", []) if not datasets or d.name in datasets]
    if datasets and len(chosen) + len(derived) != len(set(datasets)):
        unknown = set(datasets) - {d.name for d in [*chosen, *derived]}
        raise ProcessingError(f"{source_id} has no dataset(s) {sorted(unknown)}")
    raw = [
        f for f in list_raw_files(storage, getattr(module, "RAW_SOURCE", source_id))
        if (since is None or f.dt >= since) and (until is None or f.dt <= until)
    ]
    if not dry_run and not settings.db_url:
        raise ProcessingError("set BASECAST_DB_URL (the basecast Postgres database) to process")

    def go(conn) -> list[DatasetSummary]:
        out = []
        for ds in chosen:
            summary = _process_dataset(
                ds, raw, storage=storage, settings=settings, conn=conn, reprocess=reprocess,
                rebuild=rebuild, dry_run=dry_run, max_files=max_files,
                complete=since is None and until is None and max_files is None,
            )
            log.info("%s: %s", ds.name, summary.event())
            out.append(summary)
        if conn is not None:
            for sd in derived:
                summary = _build_sql_dataset(conn, sd)
                log.info("%s: %s", sd.name, summary.event())
                out.append(summary)
        return out

    if dry_run:
        return go(None)
    params = {
        "datasets": list(datasets or []), "reprocess": reprocess, "rebuild": rebuild,
        "since": since, "until": until,
    }
    with EtlRun(source_id, storage, params=params, stage="process") as run, connect(settings.db_url) as conn:
        ensure_bookkeeping(conn)
        summaries = go(conn)
        for s in summaries:
            run.event("dataset", **s.event())
            run.rows += s.rows
            run.files += s.files
            run.files_skipped += s.files_skipped
    return summaries
