"""``etl_run``: one row per pipeline execution in ``_runs/etl_run.parquet`` (status, duration, counts, events)."""

from __future__ import annotations

import io
import json
import logging
import traceback
from datetime import datetime, timezone
from types import TracebackType
from uuid import uuid4

import polars as pl

from basecast_pipelines.common.storage import Storage

log = logging.getLogger(__name__)

RUNS_KEY = "_runs/etl_run.parquet"

SCHEMA = {
    "run_id": pl.String,
    "source": pl.String,
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

    def __init__(self, source: str, storage: Storage, *, params: dict | None = None) -> None:
        self.source = source
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

    def event(self, kind: str, **data: object) -> None:
        self.events.append({"at": datetime.now(timezone.utc).isoformat(), "kind": kind, **data})
        if kind == "error":
            self.errors += 1

    def __enter__(self) -> EtlRun:
        log.info("run %s started: %s %s", self.run_id, self.source, self.params)
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
        append_run(self.storage, row)
        log.info("run %s %s: %s files, %s skipped", self.run_id, self.status, self.files, self.files_skipped)
        return False


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
    return pl.read_parquet(io.BytesIO(storage.read_bytes(RUNS_KEY)))
