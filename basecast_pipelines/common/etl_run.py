"""``etl_run``: one row per pipeline execution (status, duration, counts, events).

With ``BASECAST_DB_URL`` set, rows go to the ``etl_run`` table in Postgres (inserted as ``running``, then
updated when the run ends; a row left ``running`` by a dead process becomes ``abandoned`` when the next run
of the same source and stage starts), where the API reads them; otherwise to ``_runs/etl_run.parquet`` in the lake.
Airflow tasks tag their rows with the DAG, task and run through ``airflow_labels``. The run's start, end and
events also go to ``ops.log`` (``common/ops_log.py``) when it is on."""

from __future__ import annotations

import io
import json
import logging
import os
import traceback
from contextvars import ContextVar
from datetime import datetime, timezone
from types import TracebackType
from uuid import uuid4

import polars as pl

from basecast_pipelines.common import ops_log
from basecast_pipelines.common.storage import Storage

log = logging.getLogger(__name__)

RUNS_KEY = "_runs/etl_run.parquet"

# Set by the Airflow task before it calls a pipeline: dag_id, task_id, airflow_run_id, try_number.
airflow_labels: ContextVar[dict | None] = ContextVar("airflow_labels", default=None)

# The run in progress, so every ops.log line written inside it carries its run_id.
current_run: ContextVar[EtlRun | None] = ContextVar("current_run", default=None)

# Events emitted once per file; they stay in etl_run.events and out of ops.log.
PER_FILE_KINDS = frozenset({"unchanged", "not_modified"})

SCHEMA = {
    "run_id": pl.String,
    "source": pl.String,
    "stage": pl.String,
    "started_at": pl.Datetime("us", "UTC"),
    "finished_at": pl.Datetime("us", "UTC"),
    "duration_s": pl.Float64,
    "status": pl.String,
    "rows": pl.Int64,
    "files": pl.Int64,
    "files_skipped": pl.Int64,
    "bytes": pl.Int64,
    "error": pl.String,
    "events": pl.String,
    "params": pl.String,
}


class EtlRun:
    """Context manager. Status is ``success``, ``partial`` (some items failed) or ``failed`` (exception)."""

    def __init__(self, source: str, storage: Storage, *, params: dict | None = None, stage: str = "raw") -> None:
        self.source = source
        self.stage = stage
        self.storage = storage
        self.params = params or {}
        self.run_id = uuid4().hex
        self.started_at = datetime.now(timezone.utc)
        self.rows = 0
        self.files = 0
        self.files_skipped = 0
        self.bytes = 0
        self.errors = 0
        self.events: list[dict] = []
        self.status: str | None = None
        self._token = None

    def event(self, kind: str, **data: object) -> None:
        self.events.append({"at": datetime.now(timezone.utc).isoformat(), "kind": kind, **data})
        if kind == "error":
            self.errors += 1
            error = str(data.get("error", ""))
            ops_log.emit(
                "error", "etl.error", f"{self.source}: {error or 'item failed'}",
                error_class=ops_log.error_class_of(error) or "ItemError", context=data,
            )
        elif kind not in PER_FILE_KINDS:
            ops_log.emit("info", f"etl.{kind}", f"{self.source} {kind}: {json.dumps(data, default=str)}", context=data)

    def __enter__(self) -> EtlRun:
        log.info("run %s started: %s %s %s", self.run_id, self.source, self.stage, self.params)
        self._token = current_run.set(self)
        ops_log.emit("info", "etl_run.start", f"{self.source} {self.stage} started", context={"params": self.params})
        db_url = os.environ.get("BASECAST_DB_URL")
        if db_url:
            abandoned = _pg_insert_running(db_url, self)
            if abandoned:
                ops_log.emit(
                    "warn", "etl_run.abandoned",
                    f"{self.source} {self.stage}: {abandoned} earlier run(s) never finished; marked abandoned",
                )
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> bool:
        error = None
        if exc is not None:
            self.status = "failed"
            error = "".join(traceback.format_exception_only(exc_type, exc)).strip()
        else:
            self.status = "partial" if self.errors else "success"
        finished_at = datetime.now(timezone.utc)
        row = {
            "run_id": self.run_id,
            "source": self.source,
            "stage": self.stage,
            "started_at": self.started_at,
            "finished_at": finished_at,
            "duration_s": (finished_at - self.started_at).total_seconds(),
            "status": self.status,
            "rows": self.rows,
            "files": self.files,
            "files_skipped": self.files_skipped,
            "bytes": self.bytes,
            "error": error,
            "events": json.dumps(self.events, default=str),
            "params": json.dumps(self.params, default=str),
        }
        db_url = os.environ.get("BASECAST_DB_URL")
        if db_url:
            _pg_finish(db_url, row)
        else:
            append_run(self.storage, row)
        log.info("run %s %s: %s files, %s skipped", self.run_id, self.status, self.files, self.files_skipped)
        self._log_end(row, (exc_type, exc, tb) if exc is not None else None)
        if self._token is not None:
            current_run.reset(self._token)
        return False

    def _log_end(self, row: dict, exc_info) -> None:
        counts = {k: row[k] for k in ("rows", "files", "files_skipped", "bytes")} | {"item_errors": self.errors}
        summary = f"{self.files} files, {self.files_skipped} skipped, {self.rows} rows in {row['duration_s']:.1f} s"
        if self.status == "failed":
            ops_log.emit(
                "error", "etl_run.failed", f"{self.source} {self.stage} failed: {row['error']}",
                exc_info=exc_info, duration_ms=round(row["duration_s"] * 1000), context=counts,
            )
        else:
            level = "warn" if self.status == "partial" else "info"
            detail = f", {self.errors} item error(s)" if self.errors else ""
            ops_log.emit(
                level, f"etl_run.{self.status}", f"{self.source} {self.stage}: {summary}{detail}",
                duration_ms=round(row["duration_s"] * 1000), context=counts,
            )
        # Airflow may end the task process without atexit: write the run's lines now.
        ops_log.flush()


def _labels() -> dict:
    labels = airflow_labels.get() or {}
    return {k: labels.get(k) for k in ("dag_id", "task_id", "airflow_run_id", "try_number")}


def _pg_insert_running(db_url: str, run: EtlRun) -> int:
    """Inserts the run as ``running``; returns how many earlier runs it marked ``abandoned``."""
    from basecast_pipelines.common.db import connect, ensure_bookkeeping

    with connect(db_url) as conn:
        ensure_bookkeeping(conn)
        # One run per source and stage at a time (max_active_runs=1): a row still "running" from before
        # belongs to a process that died (lost connection, killed task) and never wrote its end.
        abandoned = conn.execute(
            """
            UPDATE etl_run SET status = 'abandoned', finished_at = now(),
                error = 'superseded by run ' || %(run_id)s || ' while still marked running'
            WHERE source = %(source)s AND stage = %(stage)s AND status = 'running'
            """,
            {"run_id": run.run_id, "source": run.source, "stage": run.stage},
        )
        conn.execute(
            """
            INSERT INTO etl_run (run_id, source, stage, dag_id, task_id, airflow_run_id, try_number,
                                 started_at, status, params)
            VALUES (%(run_id)s, %(source)s, %(stage)s, %(dag_id)s, %(task_id)s, %(airflow_run_id)s,
                    %(try_number)s, %(started_at)s, 'running', %(params)s)
            """,
            {
                "run_id": run.run_id,
                "source": run.source,
                "stage": run.stage,
                "started_at": run.started_at,
                "params": json.dumps(run.params, default=str),
                **_labels(),
            },
        )
    return abandoned.rowcount


def _pg_finish(db_url: str, row: dict) -> None:
    from basecast_pipelines.common.db import connect

    with connect(db_url) as conn:
        conn.execute(
            """
            UPDATE etl_run SET finished_at = %(finished_at)s, duration_s = %(duration_s)s, status = %(status)s,
                rows = %(rows)s, files = %(files)s, files_skipped = %(files_skipped)s, bytes = %(bytes)s,
                error = %(error)s, events = %(events)s
            WHERE run_id = %(run_id)s
            """,
            row,
        )


def append_run(storage: Storage, row: dict) -> None:
    new = pl.DataFrame([row], schema=SCHEMA)
    with storage.locked(RUNS_KEY):
        if storage.exists(RUNS_KEY):
            new = pl.concat([read_runs(storage), new], how="vertical")
        buffer = io.BytesIO()
        new.write_parquet(buffer)
        storage.write_bytes(RUNS_KEY, buffer.getvalue(), overwrite=True)


def read_runs(storage: Storage) -> pl.DataFrame:
    if not storage.exists(RUNS_KEY):
        return pl.DataFrame(schema=SCHEMA)
    runs = pl.read_parquet(io.BytesIO(storage.read_bytes(RUNS_KEY)))
    if "stage" not in runs.columns:  # rows written before the processing stage existed
        runs = runs.with_columns(pl.lit("raw").alias("stage"))
    return runs.select(list(SCHEMA))
