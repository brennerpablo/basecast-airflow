"""``basecast`` command line: run a source, list sources, write the data inventory."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from basecast_pipelines.common import ops_log
from basecast_pipelines.common.http import HttpClient
from basecast_pipelines.common.inventory import render_markdown, runs_table
from basecast_pipelines.common.raw import register_local_file
from basecast_pipelines.common.storage import storage_from_uri
from basecast_pipelines.common.validate import audit_raw
from basecast_pipelines.config import PROJECT_ROOT, load_settings, local_today
from basecast_pipelines.sources import SOURCE_MODULES, UnknownSourceError, describe, get_source

app = typer.Typer(no_args_is_help=True, help="basecast data pipelines (raw ingestion and processing).")


def _parse_option(raw: str) -> tuple[str, object]:
    key, sep, value = raw.partition("=")
    if not sep:
        raise typer.BadParameter(f"expected key=value, got {raw!r}")
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return key, lowered == "true"
    if value.lstrip("-").isdigit():
        return key, int(value)
    return key, value


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    ops_log.install()


@app.command()
def sources() -> None:
    """List the source ids in runbook order."""
    for source_id in SOURCE_MODULES:
        typer.echo(f"{source_id:26} {describe(source_id)[:110]}")


@app.command()
def run(
    source: Annotated[str, typer.Argument(help="Source id (see `basecast sources`).")],
    since: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Only dt >= this date.")] = None,
    until: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Only dt <= this date.")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="List what would be downloaded; write nothing.")] = False,
    opt: Annotated[list[str] | None, typer.Option("--opt", help="Source option key=value (repeatable).")] = None,
) -> None:
    """Discover and download a source's raw files into the lake."""
    try:
        module = get_source(source)
    except UnknownSourceError:
        raise typer.BadParameter(f"unknown source {source!r}; see `basecast sources`") from None
    settings = load_settings()
    storage = storage_from_uri(settings.storage_root)
    options = dict(_parse_option(o) for o in opt or [])
    with HttpClient(user_agent=settings.user_agent) as http:
        try:
            files = module.run(
                storage=storage,
                http=http,
                since=since.date() if since else None,
                until=until.date() if until else None,
                dry_run=dry_run,
                **options,
            )
        except Exception as exc:
            typer.secho(f"{source} failed: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(1) from exc
    if dry_run:
        for f in files:
            typer.echo(f"{f.dt}  {f.filename or f.url.rsplit('/', 1)[-1]}  <- {f.url}")
        typer.echo(f"{len(files)} files would be considered (already-fetched ones are skipped at run time)")


@app.command()
def inventory(
    out: Annotated[Path, typer.Option(help="Markdown output path.")] = PROJECT_ROOT / "docs" / "data-inventory.md",
) -> None:
    """Summarize the lake (raw files per source, last run status) into docs/data-inventory.md."""
    storage = storage_from_uri(load_settings().storage_root)
    out.write_text(render_markdown(storage))
    typer.echo(f"wrote {out}")


@app.command()
def runs(last: Annotated[int, typer.Option(help="How many runs to show.")] = 20) -> None:
    """Show the most recent etl_run rows."""
    storage = storage_from_uri(load_settings().storage_root)
    table = runs_table(storage).sort("started_at", descending=True).head(last)
    with pl.Config(tbl_rows=last, tbl_width_chars=160, fmt_str_lengths=60):
        typer.echo(table)


@app.command()
def audit() -> None:
    """Check raw integrity: every manifest entry present, sha256/size match, file signature fits its type."""
    storage = storage_from_uri(load_settings().storage_root)
    checked, problems = audit_raw(storage)
    for p in problems:
        typer.echo(f"{p.key}: {p.problem}")
    typer.echo(f"{checked} files checked, {len(problems)} problems")
    if problems:
        raise typer.Exit(1)


@app.command()
def register(
    source: Annotated[str, typer.Argument(help="Raw source id, e.g. lbnl_queued_up.")],
    path: Annotated[Path, typer.Argument(exists=True, dir_okay=False, help="File downloaded by hand.")],
    dt: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Snapshot date (default: today).")] = None,
    origin_url: Annotated[str | None, typer.Option(help="Page or URL it was downloaded from.")] = None,
    note: Annotated[str | None, typer.Option(help="Free-text provenance note.")] = None,
) -> None:
    """Register a manually downloaded file as a raw snapshot (copied to raw/source=<id>/dt=<date>/, manifest origin: manual)."""
    storage = storage_from_uri(load_settings().storage_root)
    key = register_local_file(
        storage, source, path, dt=dt.date() if dt else local_today(), origin_url=origin_url, note=note
    )
    typer.echo(f"stored {key}" if key else "same content already registered; nothing written")


@app.command()
def process(
    source: Annotated[str, typer.Argument(help="Source id with a parser (see `basecast datasets`).")],
    dataset: Annotated[list[str] | None, typer.Option("--dataset", "-d", help="Only these datasets (repeatable).")] = None,
    since: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Only raw dt >= this date.")] = None,
    until: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Only raw dt <= this date.")] = None,
    reprocess: Annotated[bool, typer.Option("--reprocess", help="Re-parse every selected file, ignoring the state.")] = False,
    rebuild: Annotated[bool, typer.Option("--rebuild", help="Drop the tables first (schema changes); implies --reprocess.")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Parse and summarize; write nothing.")] = False,
    max_files: Annotated[int | None, typer.Option(help="Parse at most this many files per dataset (dry runs).")] = None,
    sample_rows: Annotated[int, typer.Option(help="Rows of each dataset to print in a dry run.")] = 5,
) -> None:
    """Parse a source's raw files into its datasets (Postgres, or Parquet + BigQuery for the large ones)."""
    from basecast_pipelines.parsers import NoParserError
    from basecast_pipelines.processing.runner import run_process

    settings = load_settings()
    storage = storage_from_uri(settings.storage_root)
    try:
        summaries = run_process(
            source, storage=storage, settings=settings, datasets=dataset, reprocess=reprocess, rebuild=rebuild,
            dry_run=dry_run, since=since.date() if since else None, until=until.date() if until else None,
            max_files=max_files,
        )
    except NoParserError:
        raise typer.BadParameter(f"{source!r} has no parser yet; see `basecast datasets`") from None
    for s in summaries:
        typer.echo(f"{s.dataset} -> {s.target} ({s.mode}): {s.rows:,} rows from {s.files} files, "
                   f"{s.files_skipped} unchanged{' (skipped)' if s.skipped else ''}")
        if dry_run:
            for name, dtype in s.schema.items():
                typer.echo(f"    {name:32} {dtype}")
            if s.sample is not None:
                with pl.Config(tbl_rows=sample_rows, tbl_cols=-1, tbl_width_chars=200, fmt_str_lengths=40):
                    typer.echo(s.sample.head(sample_rows))


@app.command()
def datasets() -> None:
    """List every dataset produced by the parsers, with its target and write mode."""
    from basecast_pipelines.parsers import get_parser, processable_sources

    for source_id in processable_sources():
        module = get_parser(source_id)
        for d in getattr(module, "DATASETS", []):
            typer.echo(f"{source_id:26} {d.name:36} {d.target:9} {d.mode:8} v{d.version}")
        for d in getattr(module, "SQL_DATASETS", []):
            typer.echo(f"{source_id:26} {d.name:36} postgres  sql")


@app.command()
def registry() -> None:
    """Publish every dataset (source, target, mode, keys, partitioning) to Postgres ``dataset_registry``."""
    from basecast_pipelines.common.db import connect
    from basecast_pipelines.processing.registry import publish

    settings = load_settings()
    if not settings.db_url:
        raise typer.BadParameter("set BASECAST_DB_URL")
    with connect(settings.db_url) as conn:
        typer.echo(f"published {publish(conn)} registry rows")


@app.command("export-geo")
def export_geo(
    out_dir: Annotated[Path, typer.Option(help="Output folder (the app's public/geo/).")] = (
        PROJECT_ROOT.parent / "basecast-app" / "public" / "geo"
    ),
    tolerance: Annotated[
        float | None, typer.Option(help="Simplification tolerance in metres; default: the smallest under 1 MB.")
    ] = None,
) -> None:
    """Write the county and weather-zone GeoJSON for the app's map (reads PostGIS as basecast_reader)."""
    from basecast_pipelines.marts import geo

    tol, sizes = geo.export(out_dir, tolerance_m=tolerance)
    for name, size in sizes.items():
        typer.echo(f"{out_dir / name}: {size:,} bytes")
    typer.echo(f"tolerance {tol:g} m")


marts_app = typer.Typer(no_args_is_help=True, help="Marts: small typed tables for the API (docs/build/BUILD_A).")
app.add_typer(marts_app, name="marts")


@marts_app.command("list")
def marts_list() -> None:
    """List the marts in build order, with key and inputs."""
    from basecast_pipelines.marts.catalog import MARTS

    for m in MARTS.values():
        typer.echo(f"{m.name:34} v{m.version}  key={','.join(m.key)}  inputs={','.join(m.inputs)}")


def _echo_outcomes(outcomes) -> bool:
    ok = True
    for o in outcomes:
        state = "written" if o.written else "NOT WRITTEN"
        counts = {s: sum(c["status"] == s for c in o.checks) for s in ("passed", "failed", "skipped")}
        typer.echo(f"{o.name:34} {o.rows:>7} rows  {state:11} checks {counts}  {o.seconds:.1f} s")
        for c in o.checks:
            if c["status"] == "failed":
                ok = False
                typer.echo(f"    FAILED {c['check']}: expected {c.get('expected')!r}, got {c.get('actual')!r}"
                           f"{' ' + c['error'] if c.get('error') else ''}")
        ok = ok and o.written
    return ok


@marts_app.command("build")
def marts_build(
    names: Annotated[list[str] | None, typer.Argument(help="Mart names (see `basecast marts list`).")] = None,
    all_marts: Annotated[bool, typer.Option("--all", help="Build every mart.")] = False,
    as_of: Annotated[datetime | None, typer.Option(formats=["%Y-%m-%d"], help="Default: today (Chicago).")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Parquet to data/marts_dry/; no database.")] = False,
    allow_dirty: Annotated[
        bool, typer.Option("--allow-dirty", help="Write to Postgres from uncommitted code (not for real builds).")
    ] = False,
) -> None:
    """Build, check and write marts (Postgres public.mart_* as basecast_writer; etl_run source marts, stage model).

    Real builds run from committed code: a clean checkout of HEAD, or BASECAST_GIT_SHA set by the deploy."""
    from basecast_pipelines.common.etl_run import EtlRun
    from basecast_pipelines.marts import catalog, core
    from basecast_pipelines.marts import config as marts_config
    from basecast_pipelines.models.db import read_sql

    if not names and not all_marts:
        raise typer.BadParameter("name the marts or pass --all")
    marts = catalog.select(None if all_marts else names)
    day = as_of.date() if as_of else local_today()
    ctx = core.MartContext(day, marts_config.load(), read_sql)
    code = core.code_version(PROJECT_ROOT)
    if dry_run:
        outcomes = core.build_marts(marts, ctx, code=code, dry_dir=PROJECT_ROOT / "data" / "marts_dry")
    else:
        settings = load_settings()
        if not settings.db_url:
            raise typer.BadParameter("set BASECAST_DB_URL (basecast_writer)")
        if not core.is_clean(code) and not allow_dirty:
            raise typer.BadParameter(
                f"code version {code}: build from a clean checkout of HEAD (with BASECAST_GIT_SHA) or pass --allow-dirty"
            )
        params = {"marts": [m.name for m in marts], "as_of": day.isoformat(), "code": code, "dry_run": False}
        with EtlRun(core.SOURCE_ID, storage_from_uri(settings.storage_root), params=params, stage=core.STAGE) as run:
            outcomes = core.build_marts(marts, ctx, code=code, db_url=settings.db_url, run=run)
    typer.echo(f"as_of {day}, code {code}{' (dry run)' if dry_run else ''}")
    if not _echo_outcomes(outcomes):
        raise typer.Exit(1)


@marts_app.command("check")
def marts_check(
    names: Annotated[list[str] | None, typer.Argument(help="Mart names; default: every built mart.")] = None,
) -> None:
    """Re-run the checks on the marts in Postgres (read as basecast_reader), at the as_of each was built for."""
    import json
    from datetime import date

    from basecast_pipelines.marts import catalog, core
    from basecast_pipelines.marts import config as marts_config
    from basecast_pipelines.models.db import read_sql

    ok = True
    for mart in catalog.select(names):
        if read_sql("select to_regclass(%(t)s)::text as t", {"t": f"public.{mart.name}"})["t"][0] is None:
            typer.echo(f"{mart.name:34} not built")
            continue
        meta = read_sql("select value::text as v from mart_meta where mart = %(m)s and key = 'as_of'",
                        {"m": mart.meta_name})
        built_as_of = date.fromisoformat(json.loads(meta["v"][0])) if meta.height else local_today()
        frame = read_sql(f'select * from "{mart.name}"')
        checks = core.run_checks(mart, frame, built_as_of, marts_config.load())
        outcome = core.MartOutcome(mart.name, frame.height, True, checks, 0.0)
        ok = _echo_outcomes([outcome]) and ok
    if not ok:
        raise typer.Exit(1)
