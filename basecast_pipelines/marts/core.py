"""The mart layer: what a mart is, how it is built, checked and written (A-M1).

A :class:`Mart` builds one small table from the processed tables (``build(ctx)`` returns a polars frame). The run
(:func:`build_marts`) then:

1. stamps every row with ``model_version`` (commit sha + mart version), ``as_of`` (unless the mart has its own
   ``as_of`` column, e.g. a backtest cell's date) and ``built_at`` (UTC);
2. runs the mart's golden checks: a check carries the ``as_of`` of the doc it reproduces and is skipped at any
   other ``as_of``;
3. writes ``public.<name>`` as ``basecast_writer`` in one transaction (the table is dropped and recreated, so the
   API reads the old table until the commit and a schema change never breaks), with its ``mart_meta`` rows;
   a mart whose check fails is **not** written (the previous version keeps being served; on a first build the
   table does not exist and the API answers 503 ``mart_not_built``), and the run ends ``partial``;
4. publishes the registry rows (``source_id = "marts"``) and records one ``etl_run`` row (source ``marts``, stage
   ``model``) with ``mart.built`` / ``mart.check`` / ``mart.skipped`` events.

``--dry-run`` writes Parquet to ``data/marts_dry/`` and touches no database.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

log = logging.getLogger(__name__)

SOURCE_ID = "marts"
STAGE = "model"
META_TABLE = "mart_meta"
PROVENANCE = ("model_version", "as_of", "built_at")
CODE_PATHS = ("basecast_pipelines", "config")


# --- definitions ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    passed: bool
    expected: Any
    actual: Any


@dataclass(frozen=True)
class Check:
    """A golden number the mart must reproduce. ``as_of``: the date of the doc it comes from (the check only runs
    at that ``as_of``); None runs it at every ``as_of`` (a structural check). ``applies(config)``: False skips it
    when the build switches differ from the doc's (e.g. X5's counts assume the Q3 weights)."""

    name: str
    fn: Callable[[pl.DataFrame], CheckResult]
    as_of: date | None = None
    applies: Callable[[Mapping[str, Any]], bool] | None = None


def value_check(name: str, get: Callable[[pl.DataFrame], Any], expected: Any, *, as_of: date | None = None,
                tol: float = 0.0, applies: Callable[[Mapping[str, Any]], bool] | None = None) -> Check:
    """A check that ``get(frame)`` equals ``expected`` (numbers within ``tol``)."""

    def fn(frame: pl.DataFrame) -> CheckResult:
        actual = get(frame)
        if isinstance(expected, (int, float)) and not isinstance(expected, bool) and actual is not None:
            return CheckResult(abs(float(actual) - float(expected)) <= tol, expected, actual)
        return CheckResult(actual == expected, expected, actual)

    return Check(name, fn, as_of, applies)


@dataclass(frozen=True)
class Mart:
    """One mart table. ``inputs`` are the tables it reads, named as ``/tables`` shows them (the lineage in the
    app's /data/flow). ``meta(frame, ctx)`` returns the ``mart_meta`` values that are not rows (key -> JSON value);
    ``json_columns`` are text columns holding JSON, stored as jsonb."""

    name: str
    build: Callable[[MartContext], pl.DataFrame]
    key: tuple[str, ...]
    inputs: tuple[str, ...]
    description: str
    version: int = 1
    caveats: tuple[str, ...] = ()
    checks: tuple[Check, ...] = ()
    json_columns: tuple[str, ...] = ()
    meta: Callable[[pl.DataFrame, MartContext], Mapping[str, Any]] | None = None

    def __post_init__(self) -> None:
        if not self.name.startswith("mart_"):
            raise ValueError(f"mart tables are named mart_*: {self.name!r}")

    @property
    def meta_name(self) -> str:
        """The ``mart`` value in ``mart_meta`` (the name without ``mart_``)."""
        return self.name.removeprefix("mart_")


@dataclass
class MartContext:
    """What every mart build shares in one run: the date, the config and a cache of frames (the account universe,
    the GIS events...). ``read_sql(query, params)`` reads as ``basecast_reader``."""

    as_of: date
    config: Mapping[str, Any]
    read_sql: Callable[..., pl.DataFrame]
    cache: dict[str, Any] = field(default_factory=dict)

    def cached(self, key: str, load: Callable[[], Any]) -> Any:
        if key not in self.cache:
            self.cache[key] = load()
        return self.cache[key]


# --- provenance ----------------------------------------------------------------------------------------------


def code_version(root: Path) -> str:
    """Short commit sha of the code building the marts: ``BASECAST_GIT_SHA`` when set (the VM, or a clean export
    of HEAD), else ``git rev-parse``, with ``-dirty`` when ``basecast_pipelines/`` or ``config/`` has uncommitted
    changes; ``unknown`` without git."""
    if sha := os.environ.get("BASECAST_GIT_SHA"):
        return sha[:12]
    try:
        sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=root, check=True, capture_output=True,
                             text=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--", *CODE_PATHS], cwd=root, check=True,
                               capture_output=True, text=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"{sha}-dirty" if dirty else sha


def is_clean(version: str) -> bool:
    return not version.endswith("-dirty") and version != "unknown"


def stamp(frame: pl.DataFrame, mart: Mart, *, code: str, as_of: date, built_at: datetime) -> pl.DataFrame:
    """Add ``model_version`` (``<sha>.v<mart version>``), ``as_of`` (unless the mart has its own) and
    ``built_at``."""
    cols = [pl.lit(f"{code}.v{mart.version}").alias("model_version")]
    if "as_of" not in frame.columns:
        cols.append(pl.lit(as_of, dtype=pl.Date).alias("as_of"))
    cols.append(pl.lit(built_at, dtype=pl.Datetime("us", "UTC")).alias("built_at"))
    return frame.with_columns(cols)


def meta_rows(mart: Mart, frame: pl.DataFrame, ctx: MartContext, *, model_version: str,
              built_at: datetime) -> pl.DataFrame:
    """The ``mart_meta`` rows of one mart: its own values plus ``as_of``, ``caveats`` and ``inputs``."""
    values: dict[str, Any] = {"as_of": ctx.as_of.isoformat(), "caveats": list(mart.caveats),
                              "inputs": list(mart.inputs)}
    if mart.meta is not None:
        values |= dict(mart.meta(frame, ctx))
    return pl.DataFrame(
        {
            "mart": [mart.meta_name] * len(values),
            "key": list(values),
            "value": [json.dumps(v, default=str) for v in values.values()],
            "model_version": [model_version] * len(values),
            "as_of": [ctx.as_of] * len(values),
            "built_at": [built_at] * len(values),
        },
        schema={"mart": pl.String, "key": pl.String, "value": pl.String, "model_version": pl.String,
                "as_of": pl.Date, "built_at": pl.Datetime("us", "UTC")},
    )


# --- checks --------------------------------------------------------------------------------------------------


def run_checks(mart: Mart, frame: pl.DataFrame, as_of: date,
               config: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """One record per check: ``status`` passed / failed / skipped (golden check at another ``as_of``, or for
    other switches)."""
    out = []
    for check in mart.checks:
        if check.as_of is not None and check.as_of != as_of:
            out.append({"mart": mart.name, "check": check.name, "status": "skipped",
                        "reason": f"golden for as_of {check.as_of}"})
            continue
        if check.applies is not None and config is not None and not check.applies(config):
            out.append({"mart": mart.name, "check": check.name, "status": "skipped",
                        "reason": "golden for other build switches"})
            continue
        try:
            r = check.fn(frame)
            out.append({"mart": mart.name, "check": check.name, "status": "passed" if r.passed else "failed",
                        "expected": r.expected, "actual": r.actual})
        except Exception as exc:  # a check that cannot run is a failed check
            out.append({"mart": mart.name, "check": check.name, "status": "failed", "error": repr(exc)})
    return out


# --- writing -------------------------------------------------------------------------------------------------


def write_mart(conn, mart: Mart, frame: pl.DataFrame, meta: pl.DataFrame) -> int:
    """Replace ``public.<mart>`` and its ``mart_meta`` rows in one transaction."""
    from psycopg import sql

    from basecast_pipelines.common.db import write_table

    with conn.transaction():
        n = write_table(conn, mart.name, frame, mode="replace", json_columns=mart.json_columns, recreate=True)
        if conn.execute("SELECT to_regclass('public.mart_meta')").fetchone()[0] is not None:
            conn.execute(sql.SQL("DELETE FROM {} WHERE mart = %s").format(sql.Identifier(META_TABLE)),
                         (mart.meta_name,))
        write_table(conn, META_TABLE, meta, mode="by_key", key=("mart", "key"), json_columns=("value",))
    return n


def existing_marts(conn, names: Sequence[str]) -> list[str]:
    return [n for n in names if conn.execute("SELECT to_regclass(%s)", (f"public.{n}",)).fetchone()[0] is not None]


# --- the run -------------------------------------------------------------------------------------------------


@dataclass
class MartOutcome:
    name: str
    rows: int
    written: bool
    checks: list[dict[str, Any]]
    seconds: float
    reason: str | None = None


def build_marts(
    marts: Sequence[Mart],
    ctx: MartContext,
    *,
    code: str,
    db_url: str | None = None,
    dry_dir: Path | None = None,
    run=None,
) -> list[MartOutcome]:
    """Build, check and write ``marts`` in order. Exactly one of ``db_url`` (write to Postgres) and ``dry_dir``
    (Parquet only) is given. ``run``: an open :class:`~basecast_pipelines.common.etl_run.EtlRun` for the events."""
    if (db_url is None) == (dry_dir is None):
        raise ValueError("give exactly one of db_url and dry_dir")
    built_at = datetime.now(timezone.utc)
    outcomes = []
    conn = None
    if db_url:
        from basecast_pipelines.common.db import connect

        conn = connect(db_url)
    try:
        for mart in marts:
            t0 = time.monotonic()
            frame = mart.build(ctx)
            model_version = f"{code}.v{mart.version}"
            frame = stamp(frame, mart, code=code, as_of=ctx.as_of, built_at=built_at)
            checks = run_checks(mart, frame, ctx.as_of, ctx.config)
            for c in checks:
                if run is not None:
                    run.event("mart.check", **c)
            failed = [c["check"] for c in checks if c["status"] == "failed"]
            if failed:
                reason = f"golden check failed: {', '.join(failed)}; the previous version stays"
                if run is not None:
                    run.event("error", error=f"{mart.name}: {reason}")
                    run.event("mart.skipped", mart=mart.name, reason=reason)
                outcomes.append(MartOutcome(mart.name, frame.height, False, checks, time.monotonic() - t0, reason))
                continue
            meta = meta_rows(mart, frame, ctx, model_version=model_version, built_at=built_at)
            if conn is not None:
                write_mart(conn, mart, frame, meta)
            else:
                dry_dir.mkdir(parents=True, exist_ok=True)
                frame.write_parquet(dry_dir / f"{mart.name}.parquet")
                meta.write_parquet(dry_dir / f"{META_TABLE}__{mart.meta_name}.parquet")
            seconds = time.monotonic() - t0
            if run is not None:
                run.rows += frame.height
                run.files += 1
                run.event("mart.built", mart=mart.name, rows=frame.height, as_of=ctx.as_of,
                          model_version=model_version, duration_s=round(seconds, 2))
            outcomes.append(MartOutcome(mart.name, frame.height, True, checks, seconds))
        if conn is not None:
            from basecast_pipelines.processing.registry import publish_marts

            publish_marts(conn)
    finally:
        if conn is not None:
            conn.close()
    return outcomes
