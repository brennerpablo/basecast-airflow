"""``ops.log``: the one log every BaseCast service writes (basecast-get-data data contract §7), read by
the app's /ops screen.

``install()`` turns it on when ``BASECAST_DB_URL`` is set and is inert without it (tests, local runs
without a database). Two kinds of rows come from here:

- named events, through ``emit()``: ``etl_run.start|success|partial|failed|abandoned`` and one
  ``etl.<kind>`` per ``EtlRun`` event (``etl.error``, ``etl.dataset``, ``etl.http``…), all with the
  run's ``run_id``;
- every other ``WARNING`` and up from the ``basecast_pipelines`` and ``basecast_dags`` loggers, as
  ``log``. Their INFO lines (hundreds per run: one per file) stay in Airflow's task log.

Rows are queued and a daemon thread inserts them in batches over one connection per process.
``EtlRun`` calls ``flush()`` when a run ends, because Airflow may end a task process without running
``atexit``. Writing the log never raises into a pipeline: a failed insert drops that batch and says so
once on stderr."""

from __future__ import annotations

import atexit
import logging
import os
import queue
import re
import socket
import sys
import threading
import traceback
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from types import TracebackType

SERVICE = "airflow"
BATCH_ROWS = 200
MAX_MESSAGE_CHARS = 2_000
MAX_STACK_LINES = 15
LOGGERS = ("basecast_pipelines", "basecast_dags")

_REDACTED_KEYS = {"password", "token", "secret", "apikey", "api_key", "authorization", "cookie", "credentials"}
_CLASS_PREFIX = re.compile(r"^([A-Za-z_][\w.]*)(?::|$)")

COLUMNS = (
    "ts", "service", "env", "level", "event", "message", "request_id", "run_id", "user_id", "method",
    "route", "status", "duration_ms", "error_class", "error_stack", "fingerprint", "version", "host",
    "context",
)

Insert = Callable[[Sequence[dict]], None]
ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]


def _scrub(value: object, depth: int = 0) -> object:
    if depth > 4:
        return "[depth]"
    if isinstance(value, dict):
        return {
            str(k): "[redacted]" if str(k).lower() in _REDACTED_KEYS else _scrub(v, depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, list | tuple):
        return [_scrub(v, depth + 1) for v in value[:50]]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _run_context() -> dict:
    """The current run's run_id, source and stage, plus the Airflow task labels, when there are any."""
    from basecast_pipelines.common.etl_run import airflow_labels, current_run

    run = current_run.get()
    labels = airflow_labels.get() or {}
    context = {k: labels.get(k) for k in ("dag_id", "task_id", "try_number") if labels.get(k) is not None}
    if run is not None:
        context |= {"source": run.source, "stage": run.stage}
    return {"run_id": run.run_id if run is not None else None, "context": context}


def _environment() -> str:
    from basecast_pipelines.common.etl_run import airflow_labels

    return os.environ.get("BASECAST_ENV") or ("production" if airflow_labels.get() else "development")


def error_class_of(text: str | None) -> str | None:
    """``"HTTPStatusError: 429 …"`` → ``"HTTPStatusError"``; free text → None."""
    match = _CLASS_PREFIX.match((text or "").strip())
    return match.group(1) if match else None


def build_row(
    level: str,
    event: str,
    message: str,
    *,
    exc_info: ExcInfo | None = None,
    error_class: str | None = None,
    duration_ms: int | None = None,
    context: dict | None = None,
    ts: datetime | None = None,
) -> dict:
    run = _run_context()
    merged = run["context"] | (context or {})
    stack = None
    if exc_info is not None:
        error_class = error_class or exc_info[0].__name__
        lines = "".join(traceback.format_exception(*exc_info)).rstrip().split("\n")
        stack = "\n".join(lines[-MAX_STACK_LINES:])
    place = merged.get("source") or merged.get("logger") or event
    return {
        "ts": ts or datetime.now(timezone.utc),
        "service": SERVICE,
        "env": _environment(),
        "level": level,
        "event": event,
        "message": message[:MAX_MESSAGE_CHARS],
        "request_id": None,
        "run_id": run["run_id"],
        "user_id": None,
        "method": None,
        "route": None,
        "status": None,
        "duration_ms": duration_ms,
        "error_class": error_class,
        "error_stack": stack,
        "fingerprint": f"{SERVICE}:{error_class}:{place}" if error_class else None,
        "version": os.environ.get("BASECAST_VERSION"),
        "host": socket.gethostname(),
        "context": _scrub(merged) or None,
    }


def _postgres_insert(db_url: str) -> Insert:
    """One connection per process, opened on the first batch and reopened after a failure."""
    state: dict = {"conn": None}

    def insert(rows: Sequence[dict]) -> None:
        import psycopg
        from psycopg.types.json import Jsonb

        from basecast_pipelines.common.db import connect

        try:
            if state["conn"] is None or state["conn"].closed:
                state["conn"] = connect(db_url)
            placeholders = ", ".join(["%s"] * len(COLUMNS))
            with state["conn"].cursor() as cur:
                cur.executemany(
                    f"INSERT INTO ops.log ({', '.join(COLUMNS)}) VALUES ({placeholders})",
                    [
                        tuple(Jsonb(r[c]) if c == "context" and r[c] is not None else r[c] for c in COLUMNS)
                        for r in rows
                    ],
                )
        except psycopg.Error:
            if state["conn"] is not None:
                state["conn"].close()
            state["conn"] = None
            raise

    return insert


class _Writer:
    """The queue and the daemon thread that drains it: whatever is queued when the thread wakes goes
    out in one insert (up to ``BATCH_ROWS``)."""

    def __init__(self, insert: Insert) -> None:
        self.insert = insert
        self.queue: queue.Queue[dict | threading.Event | None] = queue.Queue()
        self.warned = False
        self.thread = threading.Thread(target=self._loop, name="ops-log-writer", daemon=True)
        self.thread.start()

    def put(self, row: dict) -> None:
        self.queue.put(row)

    def _write(self, rows: list[dict]) -> None:
        try:
            self.insert(rows)
        except Exception as exc:  # noqa: BLE001 — the log must never take a pipeline down
            if not self.warned:
                self.warned = True
                print(f"ops.log: dropped {len(rows)} row(s), write failed: {exc}", file=sys.stderr)

    def _loop(self) -> None:
        while True:
            item = self.queue.get()
            rows: list[dict] = []
            flushed: list[threading.Event] = []
            stop = False
            while True:
                if item is None:
                    stop = True
                elif isinstance(item, threading.Event):
                    flushed.append(item)
                else:
                    rows.append(item)
                if stop or len(rows) >= BATCH_ROWS:
                    break
                try:
                    item = self.queue.get_nowait()
                except queue.Empty:
                    break
            if rows:
                self._write(rows)
            for event in flushed:
                event.set()
            if stop:
                return

    def flush(self, timeout: float = 10.0) -> None:
        """Wait until everything queued so far is written (or dropped)."""
        done = threading.Event()
        self.queue.put(done)
        done.wait(timeout)


class OpsLogHandler(logging.Handler):
    """Turns WARNING-and-up records of the pipeline loggers into ``log`` rows."""

    def __init__(self, writer: _Writer) -> None:
        super().__init__(level=logging.WARNING)
        self.writer = writer

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = "error" if record.levelno >= logging.ERROR else "warn"
            exc_info = record.exc_info if record.exc_info and record.exc_info[0] else None
            self.writer.put(
                build_row(
                    level,
                    "log",
                    record.getMessage(),
                    exc_info=exc_info,
                    context={"logger": record.name},
                    ts=datetime.fromtimestamp(record.created, timezone.utc),
                )
            )
        except Exception:  # noqa: BLE001
            self.handleError(record)


_writer: _Writer | None = None
_handler: OpsLogHandler | None = None


def install(db_url: str | None = None, *, insert: Insert | None = None) -> bool:
    """Turns ops.log on for this process; returns whether it is on. Idempotent. ``insert`` replaces the
    database in tests."""
    global _writer, _handler
    if _writer is not None:
        return True
    url = db_url or os.environ.get("BASECAST_DB_URL")
    if insert is None and not url:
        return False
    _writer = _Writer(insert or _postgres_insert(url))
    _handler = OpsLogHandler(_writer)
    for name in LOGGERS:
        logging.getLogger(name).addHandler(_handler)
    atexit.register(flush)
    return True


def uninstall() -> None:
    """Tests: flush and detach."""
    global _writer, _handler
    if _writer is None:
        return
    flush()
    _writer.queue.put(None)
    _writer.thread.join(timeout=5)
    for name in LOGGERS:
        logging.getLogger(name).removeHandler(_handler)
    _writer = _handler = None


def emit(
    level: str,
    event: str,
    message: str,
    *,
    exc_info: ExcInfo | None = None,
    error_class: str | None = None,
    duration_ms: int | None = None,
    context: dict | None = None,
) -> None:
    """Queues a named event. A no-op when ops.log is off; never raises."""
    if _writer is None:
        return
    try:
        _writer.put(
            build_row(
                level, event, message, exc_info=exc_info, error_class=error_class,
                duration_ms=duration_ms, context=context,
            )
        )
    except Exception:  # noqa: BLE001
        pass


def flush(timeout: float = 10.0) -> None:
    if _writer is not None:
        _writer.flush(timeout)
